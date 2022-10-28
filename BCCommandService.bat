@echo off
title BCCommandService
ver
echo Created By Danu Hendarto PT. Indomarco Prismatama
:cmd
cd
java -Xms1024m -jar BCCommandService.jar
goto cmd