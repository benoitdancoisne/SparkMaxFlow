val vertices = sc.textFile("../../SparkMaxFlow/data/toy-vertices.txt").
                flatMap(line => line.split(" ")).
                map(l => l.toInt)

println("Vertices:")
vertices.foreach(v => println(v))

val edges = sc.textFile("../../SparkMaxFlow/data/toy-edges.txt").
                map(line => line.split(" ")).
                map(e => ((e(0).toInt, e(1).toInt), e(2).toInt))

println("Edges:")
edges.foreach(e => println(e))