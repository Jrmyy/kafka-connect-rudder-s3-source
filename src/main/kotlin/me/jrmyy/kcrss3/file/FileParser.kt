package me.jrmyy.kcrss3.file

interface FileParser {
    fun parse(filePath: String): List<String>
}
