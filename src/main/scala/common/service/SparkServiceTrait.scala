package common.service

import model.SparkExecutor

trait SparkServiceTrait extends AbstractServiceTrait
{
  lazy val sparkExecutor: SparkExecutor = appContext.getSparkExecutor(appContext.config)
}