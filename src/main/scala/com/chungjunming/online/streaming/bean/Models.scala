package com.chungjunming.online.streaming.bean

/**
  * Created by Chungjunming on 2019/11/4.
  */
case class Register(userId: String, platformId: String, createtime: String)

case class RegisterCount(createtime: String, var curCount: Long, var totalCount: Long)

//(用户id) (课程id) (知识点id) (题目id) (是否正确 0错误 1正确)(创建时间)
case class User2PointCorrectRate(userid: String, courseId: String, pointId: String, questionId: String, isTrue: String, createtime: String)


case class LearnModel(
                       userId: Int,
                       cwareId: Int,
                       videoId: Int,
                       chapterId: Int,
                       edutypeId: Int,
                       subjectId: Int,
                       sourceType: String,
                       speed: Int,
                       ts: Long,
                       te: Long,
                       ps: Int,
                       pe: Int
                     )

