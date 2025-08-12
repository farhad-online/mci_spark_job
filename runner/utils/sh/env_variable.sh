#!/bin/bash

setup_production_variable() {
  APP_ENV="production"
}

setup_development_variable() {
  APP_ENV="development"
}

setup_test_variable() {
  APP_ENV="test"
}