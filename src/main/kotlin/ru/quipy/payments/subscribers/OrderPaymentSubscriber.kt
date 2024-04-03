package ru.quipy.payments.subscribers

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
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
import java.util.concurrent.atomic.AtomicLongArray


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

    @Autowired
    @Qualifier(ExternalServicesConfig.FIRST_PAYMENT_BEAN)
    private lateinit var thirdPaymentService: PaymentService

    @Autowired
    @Qualifier(ExternalServicesConfig.SECOND_PAYMENT_BEAN)
    private lateinit var fourthPaymentService: PaymentService


    private fun getIndex(): Int {
        return Random().nextInt(4)
    }

    private fun isReset(): Boolean {
        val value = Random().nextDouble()
        return value <= 0.2
    }

    @Autowired
    private lateinit var paymentServices: List<PaymentService>

    private var nearestTimes = AtomicLongArray(4)


    // TODO: CAS, get and set?
    private fun getNearest(): Int {
        var minTime = Long.MAX_VALUE
        var index = 0
        for (i in 0 until nearestTimes.length()) {
            val time = nearestTimes.get(i)
            if (time < minTime) {
                minTime = time
                index = i
            }
        }
        return index
    }

    private val paymentExecutor = Executors.newFixedThreadPool(16, NamedThreadFactory("payment-executor"))

    @PostConstruct
    fun init() {
        paymentServices = paymentServices.sortedBy { it.getCost }

        subscriptionsManager.createSubscriber(
            OrderAggregate::class,
            "payments:order-subscriber",
            retryConf = RetryConf(1, RetryFailedStrategy.SKIP_EVENT)
        ) {
            `when`(OrderPaymentStartedEvent::class) { event ->
                paymentExecutor.submit {
                    val createdEvent = paymentESService.create {
                        it.create(event.paymentId, event.orderId, event.amount)
                    }
                    logger.info("Payment ${createdEvent.paymentId} for order ${event.orderId} created.")
                    DefaultPaymentServiceSelector.processingPaymentOperation(
                        paymentServices = paymentServices,
                        isReset = isReset(),
                        getNearest = getNearest(),
                        nearestTimes = nearestTimes,
                        event = event,
                        createdEvent = createdEvent,
                        paymentESService = paymentESService,
                    )
                }
            }
        }
    }
}