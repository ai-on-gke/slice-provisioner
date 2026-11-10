# Slice Provisioner Metrics

## Metrics Catalog

See `metrics.yaml` for a comprehensive catalog of metrics emitted by the Slice
Scheduler including their type, units, description, and labels. Note that
`metrics.yaml` is purely for documentation and changes to it will not affect how
the metrics are recorded or collected.

Standard controller-runtime metrics are also available (e.g.
workqueue_depth).

When using the Prometheus Operator to collect the metrics, the metrics will have
all the standard labels attached as well (e.g. pod, namespace, etc.).