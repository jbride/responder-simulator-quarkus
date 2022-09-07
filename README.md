### responder-simulator-quarkus

Simulates the movement of responders based on the messages received from the mission service.

Implemented with Quarkus

#### Implementation details

The service uses KStreams to build a local materialized view of the responders by handling _ResponderCreatedEvent_ and _ResponderDeletedEvent_ messages.
KStreams is configured to use Infinispan as repository for the materialized view.
