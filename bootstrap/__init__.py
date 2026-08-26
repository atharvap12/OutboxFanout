"""One-shot setup of the SNS topic, the three SQS queues, and their wiring.

Separate from the relay because infrastructure and application are different
jobs with different lifetimes: this runs once and exits, the relay runs forever.
"""
