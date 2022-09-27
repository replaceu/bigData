package foreo.utils

import io.searchbox.client.{JestClient, JestClientFactory}
import io.searchbox.client.config.HttpClientConfig
import io.searchbox.core.{Bulk, DocumentResult, Get, Index, Search, SearchResult}


object ForeoElasticSearchUtils {
  //声明Jest客户端工厂
  private var jestFactory:JestClientFactory = null

  //提供获取Jest客户端的方法
  def getJestClient(): JestClient ={
    if(jestFactory == null){
      //创建Jest客户端工厂对象
      build()
    }
    jestFactory.getObject
  }
  def build(): Unit = {
    jestFactory = new JestClientFactory
    jestFactory.setHttpClientConfig(new HttpClientConfig
      .Builder("http://hadoop202:9200")
      .multiThreaded(true)
      .maxTotalConnection(20)
      .connTimeout(10000)
      .readTimeout(1000).build())
  }

  //向elasticSearch中批量插入数据
  def bulkInsert(infoList: List[(String,Any)], indexName: String): Unit = {
    if(infoList!=null && infoList.size!= 0){
      //获取客户端
      val jestClient = getJestClient()
      val bulkBuilder: Bulk.Builder = new Bulk.Builder()
      for ((id,dauInfo) <- infoList) {
        val index: Index = new Index.Builder(dauInfo)
          .index(indexName)
          .id(id)
          .`type`("_doc")
          .build()
        bulkBuilder.addAction(index)
      }
      //创建批量操作对象
      val bulk: Bulk = bulkBuilder.build()
      val bulkResult = jestClient.execute(bulk)
      jestClient.close()
    }
  }

}
