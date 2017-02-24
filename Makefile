all:
	#./build/mvn -T 8 -Pyarn -Phadoop-2.4 -Dhadoop.version=2.4.0 -DskipTests clean package
	./build/mvn -T 8 -Pyarn -Phadoop-2.4 -Dhadoop.version=2.4.0 -DskipTests package
