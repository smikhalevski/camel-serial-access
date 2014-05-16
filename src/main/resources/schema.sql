create table [locks] (
  lock_id varchar(255) not null primary key,
  consumer_id varchar(255) not null,
  expires datetime not null,
  exclusive bit default 0
  )