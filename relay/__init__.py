"""Outbox Relay — the courier that carries events from the database to SNS.

The office this system models:

    orders table    the sales ledger
    outbox table    the OUT-TRAY — letters written, waiting to be posted
    THIS PACKAGE    the mail clerk who checks the tray every 2 seconds,
                    carries letters to the post office, and ticks them off
    SNS topic       the post office
    SQS queues      the recipients' pigeonholes (Phase 3)

Three steps, repeated forever: look in the tray, post one letter, tick it off.
Everything here is those steps plus handling the ways the world interrupts
between them.

Why a SEPARATE PROCESS from the Order Service: a crashed clerk must never stop
the shop from making sales, and the clerk must be restartable on its own —
Phase 6's Scenario A kills it mid-errand and proves nothing is lost.

The pattern's formal name is the POLLING PUBLISHER.
https://microservices.io/patterns/data/polling-publisher.html
"""
