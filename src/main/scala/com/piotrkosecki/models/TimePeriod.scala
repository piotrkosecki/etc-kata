package com.piotrkosecki.models


case class TimePeriod(from: Int, to: Int) {

  assert(from >= 0 && from <= 23)
  assert(to >= 0 && to <= 23)

}
