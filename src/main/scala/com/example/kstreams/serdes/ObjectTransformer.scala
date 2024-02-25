package com.example.kstreams.serdes

trait ObjectTransformer[-I, +R] {
  def transform(data: I): R
}
