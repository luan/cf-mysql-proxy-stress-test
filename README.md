# CF MySQL Proxy Stress Test

## Base

- create database
- create a table
- insert bunch of data in table

## Scenario 1

Dropping the leader when no queries are running

- read stuff from the database
- drop the leader
- check that same stuff is in the database

## Scenario 2

Dropping the leader when READ queries are running

- read stuff from the database
- read stuff from the database (continuously in the background)
  - drop the leader
- check that same stuff is in the database

## Scenario 3

Dropping the leader when WRITE queries are running

- read stuff from the database
- insert stuff from the database (continuously in the background)
  - drop the leader
- check that same stuff is in the database
- check that new stuff is in the database

## Scenario 4-5

  Scenario 2-3 in parallel

## Scenario 6

  Scenario 4-5 at the same time

## Cleanup

- drop database
