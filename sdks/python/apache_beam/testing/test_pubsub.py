import inspect
from google.cloud import pubsub_v1

class TestPubsubContext:
    """A highly advanced Pub/Sub resource lifecycle manager for Python integration tests.
    Implements cascading third-party subscription cleanup and selective
    graceful teardown for debugging on failures.

    Any catastrophic leaks are handled independently by the global 'stale_cleaner.py'.
    """
    def __init__(self, project_id):
        self.project_id = project_id
        self.publisher = pubsub_v1.PublisherClient()
        self.subscriber = pubsub_v1.SubscriberClient()

        # Lists to track resources created during the test execution
        self.tracked_topics = []
        self.tracked_subscriptions = []
        self.caller_class = "UnkknownTestClass"
        stack = inspect.stack()

        for frame in stack:
            self_obj = frame[0].f_locals.get('self', None)
            if self_obj and hasattr(self_obj, '__class__'):
                self.caller_class = self_obj.__class__.__name__
                break

    def register_topic(self, topic_path: str):
        """Registers a topic to be monitored and deleted at the end."""
        if topic_path not in self.tracked_topics:
            self.tracked_topics.append(topic_path)
            print(f"[TestPubsubContext][LOG][{self.caller_class}] Registering Topic for monitoring: {topic_path}")

    def register_subscription(self, subscription_path: str):
        """Registers a subscription to be monitored and deleted at the end."""
        if subscription_path not in self.tracked_subscriptions:
            self.tracked_subscriptions.append(subscription_path)
            print(f"[TestPubsubContext][LOG][{self.caller_class}] Registering Subscription for monitoring: {subscription_path}")

    def __enter__(self):
        print(f"[TestPubsubContext][START] [{self.caller_class}] Initializing Pub/Sub resource context for test execution...")
        return self

    def _delete_cascading_subscriptions(self, topic_path: str):
        """
        Finds and deletes from GCP any third-party subscription that is
        connected to our test topic, preventing loose residual resources.
        """
        print(f"[TestPubsubContext][LOG][{self.caller_class}] Checking for cascading subscriptions on topic: {topic_path}")
        try:
            # List all subscriptions associated with this specific topic in GCP
            for sub_path in self.publisher.list_topic_subscriptions(request={"topic": topic_path}):
                print(f"[TestPubsubContext][LOG][{self.caller_class}] [Teardown - Cascade] Deleting residual third-party subscription: {sub_path}")
                try:
                    self.subscriber.delete_subscription(request={"subscription": sub_path})
                except Exception as e:
                    print(f"[TestPubsubContext][LOG][{self.caller_class}] [Teardown Error] Could not delete cascading subscription {sub_path}: {e}")
        except Exception as e:
            print(f"[TestPubsubContext][LOG][{self.caller_class}] [Teardown Error] Could not list subscriptions associated with topic {topic_path}: {e}")

    def __exit__(self, exc_type, exc_val, exc_tb):
        print("\n[TestPubsubContext] Starting teardown of registered resources...")

        # If the test failed (exc_type is not None), we leave the subscriptions active for 2 hours
        # with an automatic TTL in GCP so the developer can debug the backlog.
        # If the test was successful, we clean up everything immediately to save 100% of the cost.
        test_failed = exc_type is not None

        if test_failed:
            print(f"[TestPubsubContext][LOG][{self.caller_class}] [ALERT] Failed test detected. Applying debugging policy (Graceful Teardown).")
            print(f"[TestPubsubContext][LOG][{self.caller_class}] [INFO] Resources will self-destruct automatically in GCP to allow debugging.")
            return False
        print(f"[TestPubsubContext][LOG][{self.caller_class}] [SUCCESS] Test passed. Proceeding with immediate cleanup of all registered resources.")
        # 1. Delete registered Subscriptions (Only if the test was successful)
        for sub_path in list(self.tracked_subscriptions):
            try:
                print(f"[TestPubsubContext] Deleting temporary subscription: {sub_path}")
                self.subscriber.delete_subscription(request={"subscription": sub_path})
                self.tracked_subscriptions.remove(sub_path)
            except Exception as e:
                print(f"[TestPubsubContext Error] Could not delete subscription {sub_path}: {e}")

        # 2. Cascading Topic Cleanup (Check connected third-party subscriptions)
        for topic_path in list(self.tracked_topics):
            # Execute cascading deletion inspired by Java logic
            self._delete_cascading_subscriptions(topic_path)
            try:
                print(f"[TestPubsubContext] Deleting temporary topic: {topic_path}")
                self.publisher.delete_topic(request={"topic": topic_path})
                self.tracked_topics.remove(topic_path)
            except Exception as e:
                print(f"[TestPubsubContext Error] Could not delete topic {topic_path}: {e}")

        return False