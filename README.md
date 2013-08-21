*** Simple command line beanstalkd client

Allows two modes of operation: push and pull.

- Push: Reads from stdin pipe and writes it to the specified beanstalkd tube
- Pull: Blocks and reserves a job from beanstalkd. Once read, it deletes the job from the specified tube.
