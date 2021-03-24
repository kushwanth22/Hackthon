package org.realtime.sparkcore

case class Insure(
    IssuerId:String,
    IssuerId2:String,
    BusinessDate:String, //java.sql.Date,
    StateCode:String,
    SourceName:String,
    NetworkName:String,
    NetworkURL:String,
    custnum:String,
    MarketCoverage:String,
    DentalOnlyPlan:String
)