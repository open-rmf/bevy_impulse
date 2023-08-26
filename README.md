# :warning: Under Construction :warning:

# Reactive Programming for Bevy

Plugins that facilitate sophisticated [reactive programming](https://en.wikipedia.org/wiki/Reactive_programming) for the [bevy](https://bevyengine.org/) ECS.

## Key Concepts

### Topics

Topics are used to construct anonymous many-to-many publisher-subscriber relationships.
Publishers produce data and associate that data with a topic (key).
Subscribers listen for data on a topic (key) or across a range of topics (pattern or key range) and specify a callback that will be triggered when a message arrives on a relevant topic.

### Services

Services create one-to-one relationships between a requester and a responder.
A service will take in request data and then asynchronously provide a response at a later time.
The requester provides a callback that will be triggered once the response is available.

Additionally we support output streams for services.
While running, services can stream out data which may be related to progress of the service or other information that may be relevant to the requester.
The requester can subscribe to these streams by providing a separate callback for each stream type.
