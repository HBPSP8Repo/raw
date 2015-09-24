package raw.utils

import com.codahale.metrics.{JmxReporter, Slf4jReporter}
import com.typesafe.scalalogging.StrictLogging

object ExecutorMetrics extends StrictLogging {
  /** The application wide metrics registry. */
  val registry = new com.codahale.metrics.MetricRegistry()
  logger.info("Reporting performance metrics to JMX")
  val jmxReporter = JmxReporter.forRegistry(registry).build();
  jmxReporter.start
}

trait Instrumented extends nl.grons.metrics.scala.InstrumentedBuilder {
  val metricRegistry = ExecutorMetrics.registry
}