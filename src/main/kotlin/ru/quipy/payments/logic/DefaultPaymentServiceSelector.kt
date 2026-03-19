package ru.quipy.payments.logic

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import ru.quipy.common.utils.NonBlockingOngoingWindow
import ru.quipy.core.EventSourcingService
import ru.quipy.orders.api.OrderPaymentStartedEvent
import ru.quipy.payments.api.PaymentAggregate
import ru.quipy.payments.api.PaymentCreatedEvent
import ru.quipy.payments.subscribers.OrderPaymentSubscriber
import java.time.Duration
import java.util.UUID
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.AtomicLongArray

@Service
abstract class DefaultPaymentServiceSelector {
    companion object {
        val logger: Logger = LoggerFactory.getLogger(OrderPaymentSubscriber::class.java)

        fun processingPaymentOperation(
            paymentServices: List<PaymentService>,
            isReset: Boolean,
            getNearest: Int,
            nearestTimes: AtomicLongArray,
            event: OrderPaymentStartedEvent,
            createdEvent: PaymentCreatedEvent,
            paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>
        ) {
            var paymentProcessed = false

            for (index in paymentServices.indices) {
                if (paymentServices[index].getQueries.remainingCapacity() == 0) {
                    continue
                }
                if (paymentServices[index].window.putIntoWindow()::class == NonBlockingOngoingWindow.WindowResponse.Success::class) {
                    if (paymentServices[index].rateLimiter.tick()) {
                        try {
                            nearestTimes[index] =
                                (System.currentTimeMillis() + (1.0 / paymentServices[index].calculateSpeed())).toLong()
                            paymentServices[index].enqueuePayment(
                                createdEvent.paymentId, event.amount, event.createdAt
                            )
                            paymentProcessed = true
                            break
                        } finally {
                            paymentServices[index].window.releaseWindow()
                        }
                    } else {
                        paymentServices[index].window.releaseWindow()
                    }
                }
            }

            if (!paymentProcessed) {
                paymentESService.update(createdEvent.paymentId) {
                    val transactionId = UUID.randomUUID()
                    logger.warn("${createdEvent.paymentId} failed")
                    it.logSubmission(
                        success = true, transactionId, now(), Duration.ofMillis(now() - event.createdAt)
                    )
                    it.logProcessing(success = false, processedAt = now(), transactionId = transactionId)
                }
            }
        }

    }
}