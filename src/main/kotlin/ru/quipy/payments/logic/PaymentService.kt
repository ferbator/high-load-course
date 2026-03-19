package ru.quipy.payments.logic

import kotlinx.coroutines.Job
import java.time.Duration
import java.util.*
import ru.quipy.common.utils.CoroutineRateLimiter
import ru.quipy.common.utils.NonBlockingOngoingWindow
import java.util.concurrent.ArrayBlockingQueue

interface PaymentService {

    val rateLimiter: CoroutineRateLimiter
    val window: NonBlockingOngoingWindow
    val getCost: Int
    val getQueries: ArrayBlockingQueue<PaymentProperties>

    /**
     * Submit payment request to external service.
     */
    fun submitPaymentRequest(transactionId: UUID, paymentId: UUID, amount: Int, paymentStartedAt: Long): Job
    fun canWait(paymentStartedAt: Long): Boolean
    fun notOverTime(paymentStartedAt: Long): Boolean
    fun calculateSpeed(): Double
    fun enqueuePayment(paymentId: UUID, amount: Int, paymentStartedAt: Long): Job
}

interface PaymentExternalService : PaymentService

/**
 * Describes properties of payment-provider accounts.
 */
data class ExternalServiceProperties(
    val serviceName: String,
    val accountName: String,
    val parallelRequests: Int,
    val rateLimitPerSec: Int,
    val cost: Int,
    val request95thPercentileProcessingTime: Duration = Duration.ofSeconds(11)
)

/**
 * Describes properties of payments.
 */
data class PaymentProperties(
    val id: UUID, val amount: Int, val startedAt: Long
)

/**
 * Describes response from external service.
 */
class ExternalSysResponse(
    val result: Boolean,
    val message: String? = null,
)