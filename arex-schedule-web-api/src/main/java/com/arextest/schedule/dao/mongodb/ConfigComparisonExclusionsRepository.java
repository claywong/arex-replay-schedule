package com.arextest.schedule.dao.mongodb;

import com.arextest.schedule.model.dao.mongodb.ConfigComparisonExclusionsCollection;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Repository;

/**
 * ConfigComparisonExclusions集合的Repository
 */
@Slf4j
@Repository
public class ConfigComparisonExclusionsRepository implements RepositoryField {

  @Autowired
  private MongoTemplate mongoTemplate;

  /**
   * 根据应用ID查询排除配置
   * 
   * @param appId 应用ID
   * @return 排除配置列表
   */
  public List<ConfigComparisonExclusionsCollection> queryByAppId(String appId) {
    try {
      Query query = Query.query(
          Criteria.where("appId").is(appId)
      );
      
      List<ConfigComparisonExclusionsCollection> results = 
          mongoTemplate.find(query, ConfigComparisonExclusionsCollection.class);
      
      LOGGER.debug("查询排除配置，appId: {}, 结果数量: {}", appId, results.size());
      return results;
      
    } catch (Exception e) {
      LOGGER.error("查询排除配置异常，appId: {}, error: {}", appId, e.getMessage(), e);
      throw e;
    }
  }

  /**
   * 根据应用ID和操作ID查询排除配置
   * 
   * @param appId 应用ID
   * @param operationId 操作ID
   * @return 排除配置列表
   */
  public List<ConfigComparisonExclusionsCollection> queryByAppIdAndOperationId(String appId, String operationId) {
    try {
      Criteria criteria = Criteria.where("appId").is(appId);
      
      if (operationId != null) {
        criteria.and("operationId").is(operationId);
      }
      
      Query query = Query.query(criteria);
      
      List<ConfigComparisonExclusionsCollection> results = 
          mongoTemplate.find(query, ConfigComparisonExclusionsCollection.class);
      
      LOGGER.debug("查询排除配置，appId: {}, operationId: {}, 结果数量: {}", appId, operationId, results.size());
      return results;
      
    } catch (Exception e) {
      LOGGER.error("查询排除配置异常，appId: {}, operationId: {}, error: {}", appId, operationId, e.getMessage(), e);
      throw e;
    }
  }
} 