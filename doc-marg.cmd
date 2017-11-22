:restart
rem markdown syntax at https://daringfireball.net/projects/markdown/syntax

rem "call lein marg" works fine
rem -m seems to create syntax error
rem call lein marg -m -D -v -n
rem call lein marg -D "desc" -v "1.0" -n "name"
rem lein marg -m

call java -jar C:/data4/clojure/marginalia/target/marginalia-0.9.0-standalone.jar -m -r c:/data4/clojure/hashdb/ ./src ./test
pause
goto :restart
