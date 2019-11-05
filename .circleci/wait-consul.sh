#!/bin/bash

echo "Waiting Consul to launch on 8500..."

while ! nc -z localhost 8500; do
    sleep 1
done

sleep 3

echo "Consul proxy launched"
