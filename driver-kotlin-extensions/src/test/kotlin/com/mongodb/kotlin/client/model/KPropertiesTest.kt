package com.mongodb.kotlin.client.model

import kotlinx.serialization.SerialName
import org.bson.codecs.pojo.annotations.BsonId
import org.bson.codecs.pojo.annotations.BsonProperty
import org.junit.Test

import java.math.BigDecimal
import java.util.Locale
import kotlin.test.assertEquals

class KPropertiesTest {

    data class Restaurant(
        @BsonId
        val a: String,
        @BsonProperty("b")
        val bsonProperty: String,
        @SerialName("c")
        val serialName: String,
        val stringList: List<String>,
        val map: Map<String, String>,
        @BsonProperty("nested")
        val subDocument: Restaurant?
    )

    @Test
    fun testPath() {
        assertEquals("_id", Restaurant::a.path())
        assertEquals("b", Restaurant::bsonProperty.path())
        assertEquals("c", Restaurant::serialName.path())
        assertEquals("stringList", Restaurant::stringList.path())
        assertEquals("map", Restaurant::map.path())
        assertEquals("nested", Restaurant::subDocument.path())
    }

    @Test
    fun testDivOperator() {



    }


    // ----------

    data class Friend(
        val name: String,
        val i: Int,
        @BsonId val id: String,
        val gift: Gift,
        val allGifts: List<Gift>,
        val localeMap: Map<Locale, Gift>
    )

    data class Gift(val amount: BigDecimal)

    @Test
    fun `test array positional operator`() {
        val p = Friend::allGifts.colProperty
        assertEquals("allGifts.\$", p.posOp.path)
        assertEquals("allGifts.amount", (p / Gift::amount).path())
        assertEquals("allGifts.\$.amount", (p.posOp / Gift::amount).path())
    }

    @Test
    fun `test array all positional operator`() {
        val p = Friend::allGifts.colProperty
        assertEquals("allGifts.\$[]", p.allPosOp.path)
        assertEquals("allGifts.amount", (p / Gift::amount).path())
        assertEquals("allGifts.\$[].amount", (p.allPosOp / Gift::amount).path())
    }

    @Test
    fun `test array filtered positional operator`() {
        val p = Friend::allGifts.colProperty
        assertEquals("allGifts.\$[a]", p.filteredPosOp("a").path)
        assertEquals("allGifts.amount", (p / Gift::amount).path())
        assertEquals("allGifts.\$[a].amount", (p.filteredPosOp("a") / Gift::amount).path())
    }

    @Test
    fun `test map projection`() {
        val p = Friend::localeMap.mapProperty
        assertEquals("localeMap", p.path)
        assertEquals("localeMap.en", p.keyProjection(Locale.ENGLISH).path())
        assertEquals("localeMap.en.amount", (p.keyProjection(Locale.ENGLISH) / Gift::amount).path())
    }

    @Test
    fun `test array index property`() {
        val p = Friend::allGifts.colProperty
        assertEquals("allGifts.1.amount", (p.memberWithAdditionalPath("1") / Gift::amount).path())
    }

}
