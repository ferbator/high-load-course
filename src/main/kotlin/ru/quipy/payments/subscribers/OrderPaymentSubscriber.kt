package ru.quipy.payments.subscribers

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.stereotype.Service
import ru.quipy.common.utils.NamedThreadFactory
import ru.quipy.common.utils.NonBlockingOngoingWindow
import ru.quipy.core.EventSourcingService
import ru.quipy.orders.api.OrderAggregate
import ru.quipy.orders.api.OrderPaymentStartedEvent
import ru.quipy.payments.api.PaymentAggregate
import ru.quipy.payments.config.ExternalServicesConfig
import ru.quipy.payments.logic.*
import ru.quipy.streams.AggregateSubscriptionsManager
import ru.quipy.streams.annotation.RetryConf
import ru.quipy.streams.annotation.RetryFailedStrategy
import java.util.*
import java.util.concurrent.Executors
import javax.annotation.PostConstruct
import java.time.Duration
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger


@Service
class OrderPaymentSubscriber {

    val logger: Logger = LoggerFactory.getLogger(OrderPaymentSubscriber::class.java)

    @Autowired
    lateinit var subscriptionsManager: AggregateSubscriptionsManager

    @Autowired
    private lateinit var paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>

    @Autowired
    @Qualifier(ExternalServicesConfig.FIRST_PAYMENT_BEAN)
    private lateinit var firstPaymentService: PaymentService

    @Autowired
    @Qualifier(ExternalServicesConfig.SECOND_PAYMENT_BEAN)
    private lateinit var secondPaymentService: PaymentService

    private val paymentExecutor = Executors.newFixedThreadPool(16, NamedThreadFactory("payment-executor"))

    @PostConstruct
    fun init() {
        subscriptionsManager.createSubscriber(OrderAggregate::class, "payments:order-subscriber", retryConf = RetryConf(1, RetryFailedStrategy.SKIP_EVENT)) {
            `when`(OrderPaymentStartedEvent::class) { event ->
                paymentExecutor.submit {
                    val createdEvent = paymentESService.create {
                        it.create(event.paymentId, event.orderId, event.amount)
                    }
                    logger.info("Payment ${createdEvent.paymentId} for order ${event.orderId} created.")

                    if (!trySubmitPayment(secondPaymentService, createdEvent.paymentId, event.amount, event.createdAt)) {
                        trySubmitPayment(firstPaymentService, createdEvent.paymentId, event.amount, event.createdAt)
                    }
                }
            }
        }
    }

    private fun trySubmitPayment(paymentService: PaymentService, paymentId: UUID, amount: Int, paymentStartedAt: Long): Boolean {
        if (paymentService.notOverTime(paymentStartedAt)) {
            val windowResponse = paymentService.window.putIntoWindow()
            if (windowResponse is NonBlockingOngoingWindow.WindowResponse.Success) {
                if (paymentService.rateLimiter.tick()) {
                    paymentService.submitPaymentRequest(paymentId, amount, paymentStartedAt)
                    return true
                } else {
                    paymentService.window.releaseWindow()
                }
            }
        }
        return false
    }
}