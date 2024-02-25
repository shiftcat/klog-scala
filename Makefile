clean:
	@sbt clean

assembly:
	@sbt clean assembly

funnel:
	@scala -classpath target/klog-scala.jar com.example.kstreams.apps.FunnelApp

bucket:
	@scala -classpath target/klog-scala.jar com.example.kstreams.apps.BucketApp
