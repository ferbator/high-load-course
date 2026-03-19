package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import kotlinx.coroutines.*
import okhttp3.Dispatcher
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.*
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import ru.quipy.common.utils.CoroutineRateLimiter
import ru.quipy.common.utils.NonBlockingOngoingWindow
import java.io.IOException
import okhttp3.*
import java.util.concurrent.ArrayBlockingQueue


class PaymentExternalServiceImpl(
    private val properties: ExternalServiceProperties,
) : PaymentExternalService {

    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalServiceImpl::class.java)

        val paymentOperationTimeout = Duration.ofSeconds(80)

        val emptyBody = RequestBody.create(null, ByteArray(0))
        val mapper = ObjectMapper().registerKotlinModule()
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val cost = properties.cost
    private val requestAverageProcessingTime = properties.request95thPercentileProcessingTime
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val parallelRequests = properties.parallelRequests

    private val _rateLimiter = CoroutineRateLimiter(rateLimitPerSec, TimeUnit.SECONDS)
    override val rateLimiter: CoroutineRateLimiter
        get() = _rateLimiter

    private val _window = NonBlockingOngoingWindow(parallelRequests)
    override val window: NonBlockingOngoingWindow
        get() = _window

    override val getCost: Int
        get() = cost

    private fun maxQueries() =
        ((paymentOperationTimeout.toMillis() - requestAverageProcessingTime.toMillis())
                * calculateSpeed() * parallelRequests).toInt()

    private val _queries = ArrayBlockingQueue<PaymentProperties>(maxQueries())

    override val getQueries: ArrayBlockingQueue<PaymentProperties>
        get() = _queries

    private val queueProcessingScope = CoroutineScope(Dispatchers.IO.limitedParallelism(64))

    private val requestScope = CoroutineScope(Dispatchers.IO.limitedParallelism(64))

    @Autowired
    private lateinit var paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>

    private val httpClientExecutor = Executors.newCachedThreadPool()

    private val client = OkHttpClient.Builder().run {
        protocols(listOf(Protocol.H2_PRIOR_KNOWLEDGE))
        val dis = Dispatcher(httpClientExecutor)
        dis.maxRequestsPerHost = parallelRequests
        dis.maxRequests = parallelRequests
        dispatcher(dis)
        build()
    }

    override fun canWait(paymentStartedAt: Long): Boolean {
        return paymentOperationTimeout - Duration.ofMillis(now() - paymentStartedAt) >= requestAverageProcessingTime.multipliedBy(
            2
        )
    }

    override fun notOverTime(paymentStartedAt: Long): Boolean {
        return Duration.ofMillis(now() - paymentStartedAt) + requestAverageProcessingTime < paymentOperationTimeout
    }

    override fun calculateSpeed(): Double {
        val averageTime = requestAverageProcessingTime.toMillis().toDouble()
        return java.lang.Double.min(
            parallelRequests.toDouble() / averageTime,
            rateLimitPerSec.toDouble() / 1000
        )
    }

    override fun submitPaymentRequest(transactionId: UUID, paymentId: UUID, amount: Int, paymentStartedAt: Long) =
        requestScope.launch {
            logger.warn("[$accountName] Submit payment $paymentId. Since submission passed: ${now() - paymentStartedAt} ms.")

            logger.info("[$accountName] Submission for $paymentId , transactionId: $transactionId")

            paymentESService.update(paymentId) {
                it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
            }

            val request = Request.Builder().run {
                url("http://localhost:1234/external/process?serviceName=${serviceName}&accountName=${accountName}&transactionId=$transactionId")
                post(emptyBody)
            }.build()

            client.newCall(request).enqueue(object : Callback {
                override fun onFailure(call: Call, e: IOException) {
                    when (e) {
                        is SocketTimeoutException -> {
                            paymentESService.update(paymentId) {
                                it.logProcessing(false, now(), transactionId, reason = "Request timeout.")
                            }
                        }

                        else -> {
                            logger.error(
                                "[$accountName] FAIL: Payment failed for transactionId: $transactionId, paymentId: $paymentId",
                                e
                            )

                            paymentESService.update(paymentId) {
                                it.logProcessing(false, now(), transactionId, reason = e.message)
                            }
                        }
                    }
                }

                override fun onResponse(call: Call, response: Response) {
                    response.use {
                        val body = try {
                            mapper.readValue(response.body?.string(), ExternalSysResponse::class.java)
                        } catch (e: Exception) {
                            logger.error("[$accountName] [ERROR] Payment processed for transactionId: $transactionId, paymentId: $paymentId, response code: ${response.code}, reason: ${response.body?.string()}.")
                            ExternalSysResponse(false, e.message)
                        }

                        logger.warn("[$accountName] Payment processed for transactionId: $transactionId, paymentId: $paymentId, result: ${body.result}, message: ${body.message}, duration: ${now() - paymentStartedAt}.")

                        paymentESService.update(paymentId) {
                            it.logProcessing(body.result, now(), transactionId, reason = body.message)
                        }
                    }
                }
            })
        }

    override fun enqueuePayment(paymentId: UUID, amount: Int, paymentStartedAt: Long) = queueProcessingScope.launch {
        _queries.put(PaymentProperties(paymentId, amount, paymentStartedAt))
        logger.warn("[$accountName] Since payment $paymentId adding passed ${now() - paymentStartedAt} ms.")
    }

    private val processQueue = queueProcessingScope.launch {
        while (true) {
            if (_queries.isEmpty())
                continue

            if (window.putIntoWindow() is NonBlockingOngoingWindow.WindowResponse.Success) {
                while (!rateLimiter.tick())
                    continue
            } else {
                continue
            }

            val payment = _queries.take()
            val paymentId = payment.id
            logger.warn("[$accountName] Since payment $paymentId submission passed ${now() - payment.startedAt} ms.")
            val transactionId = UUID.randomUUID()
            logger.info("[$accountName] Submission for $paymentId passed, transaction $transactionId.")
            paymentESService.update(payment.id) {
                it.logSubmission(
                    success = true, transactionId, now(), Duration.ofMillis(now() - payment.startedAt)
                )
            }

            submitPaymentRequest(transactionId, payment.id, payment.amount, payment.startedAt)
        }
    }
}

public fun now() = System.currentTimeMillis()