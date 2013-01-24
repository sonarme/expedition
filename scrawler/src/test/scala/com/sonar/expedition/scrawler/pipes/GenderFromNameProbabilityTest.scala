package com.sonar.expedition.scrawler.pipes

import org.scalatest.FlatSpec
import org.scalatest.matchers.ShouldMatchers
import com.sonar.dossier.dto.Gender

class GenderFromNameProbabilityTest extends FlatSpec with ShouldMatchers {
    "gender classification" should "respond with high probability for a male name" in {
        val (gender, probability) = GenderFromNameProbability.gender("Paul")
        assert(gender === Gender.male)
        assert(probability > 0.7)
    }
    it should "respond with high probability for an uppercase male name" in {
        val (gender, probability) = GenderFromNameProbability.gender("PAUL")
        assert(gender === Gender.male)
        assert(probability > 0.7)
    }

    it should "respond with high probability for a female name" in {
        val (gender, probability) = GenderFromNameProbability.gender("Anna")
        assert(gender === Gender.female)
        assert(probability > 0.7)
    }

    it should "respond with unknown" in {
        val (gender, probability) = GenderFromNameProbability.gender("xyz")
        assert(gender === Gender.unknown)
        assert(probability >= 0)
    }
}
