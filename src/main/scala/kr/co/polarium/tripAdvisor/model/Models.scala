package kr.co.polarium.tripAdvisor.model

// Rating 클래스 정의(스키마 정보)
case class Ratings(userId: Long, hotelId: Long, rating: Double)

case class HotelRating(userId: Int, hotelId: Int, rating: Float)
    