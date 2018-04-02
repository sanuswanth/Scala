package com.webPage.model

case class PageView(event_id: String, collector_tstamp: Long, domain_userid: String, page_urlpath: String)
case class LinkedPageView(event_id: String, collector_tstamp: Long, domain_userid: String, page_urlpath: String, next_event_id: String)
