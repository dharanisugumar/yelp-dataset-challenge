package common.service

import common.AppContext

trait AbstractServiceTrait
{
  lazy val appContext: AppContext = AppContext()
}