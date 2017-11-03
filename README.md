# imageDownloader
Tools to download images from Google, Bing and Yahoo using a hadoop cluster.

These tools allow you to quickly download the images that appear in Google Images, Bing Images and Yahoo Images when you do different searchs.

Dependencies:
Hadoop 2.0+
Jsoup (in the hadoop cluster)

Before Executing it:
Change GenerateTrainingData.java to indicate your input (you can find an example of the input in the resources folder) and ouput
Change the PROXIES variable in MapperCollector.java to indicate your favourite proxies (or the gateway of your hadoop cluster). Using multiple Proxies you will avoid the banning from the search services.
