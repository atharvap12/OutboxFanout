"""One-shot setup of the SNS topic, the three SQS queues, and their wiring.

Separate from the relay because infrastructure and application are different
jobs with different lifetimes: this runs once and exits; the relay runs forever.

Think of it as the day the office opened: someone registered with the post
office, installed three pigeonholes, and told the sorting room which mail goes
where. Nobody does that during business hours — but if the building is rebuilt
every morning (which is exactly what LocalStack does), it has to be a checklist
you can re-run, not something a person remembers.
"""
