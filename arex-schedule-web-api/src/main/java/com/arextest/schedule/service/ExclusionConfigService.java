package com.arextest.schedule.service;

import com.arextest.common.cache.CacheProvider;
import com.arextest.schedule.common.JsonUtils;
import com.arextest.schedule.dao.mongodb.ConfigComparisonExclusionsRepository;
import com.arextest.schedule.model.converter.ConfigComparisonExclusionsConverter;
import com.arextest.schedule.model.dao.mongodb.ConfigComparisonExclusionsCollection;
import com.arextest.schedule.model.exclusion.ExclusionConfigItem;
import com.arextest.schedule.model.ReplayPlan;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.Resource;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.springframework.stereotype.Service;

/**
 * 排除配置服务，用于查询和处理字段排除配置（MongoDB + Redis缓存）
 */
@Slf4j
@Service
public class ExclusionConfigService {

  /**
   * 缓存前缀
   */
  private static final String EXCLUSION_CONFIG_CACHE_PREFIX = "exclusion.config.";
  
  /**
   * 缓存时间 - 4小时
   */
  private static final long CACHE_EXPIRE_SECONDS = 4 * 60 * 60L;

  @Resource
  private ConfigComparisonExclusionsRepository configComparisonExclusionsRepository;
  
  @Resource
  private CacheProvider redisCacheProvider;
  
  private static final ObjectMapper objectMapper = new ObjectMapper();

  /**
   * 查询指定应用的排除配置（带缓存）
   * 
   * @param appId 应用ID
   * @return 排除配置列表
   */
  public List<ExclusionConfigItem> queryExclusionConfig(String appId) {
    if (appId == null) {
      return Collections.emptyList();
    }

    // 先尝试从缓存获取
    List<ExclusionConfigItem> cachedConfig = getFromCache(appId);
    if (cachedConfig != null) {
      LOGGER.debug("从缓存获取排除配置，appId: {}, 配置数量: {}", appId, cachedConfig.size());
      return cachedConfig;
    }

    // 缓存未命中，从数据库查询
    try {
      List<ConfigComparisonExclusionsCollection> collections = 
          configComparisonExclusionsRepository.queryByAppId(appId);

      if (CollectionUtils.isEmpty(collections)) {
        LOGGER.debug("未找到排除配置，appId: {}", appId);
        List<ExclusionConfigItem> emptyList = Collections.emptyList();
        // 缓存空结果，避免频繁查询
        putToCache(appId, emptyList);
        return emptyList;
      }

      List<ExclusionConfigItem> exclusionConfigs = collections.stream()
          .filter(collection -> CollectionUtils.isNotEmpty(collection.getExclusions())) // 只返回有exclusions的配置
          .map(ConfigComparisonExclusionsConverter.INSTANCE::dtoFromDao)
          .collect(Collectors.toList());

      LOGGER.info("从数据库获取排除配置，appId: {}, 配置数量: {}", appId, exclusionConfigs.size());
      
      // 缓存查询结果
      putToCache(appId, exclusionConfigs);
      
      return exclusionConfigs;

    } catch (Exception e) {
      LOGGER.error("查询排除配置异常，appId: {}, error: {}", appId, e.getMessage(), e);
      return Collections.emptyList();
    }
  }

  /**
   * 查询指定应用和操作的排除配置（从MongoDB）
   * 
   * @param appId 应用ID
   * @param operationId 操作ID
   * @return 排除配置列表
   */
  public List<ExclusionConfigItem> queryExclusionConfig(String appId, String operationId) {
    try {
      List<ConfigComparisonExclusionsCollection> collections = 
          configComparisonExclusionsRepository.queryByAppIdAndOperationId(appId, operationId);

      if (CollectionUtils.isEmpty(collections)) {
        LOGGER.debug("未找到排除配置，appId: {}, operationId: {}", appId, operationId);
        return Collections.emptyList();
      }

      List<ExclusionConfigItem> exclusionConfigs = collections.stream()
          .filter(collection -> CollectionUtils.isNotEmpty(collection.getExclusions())) // 只返回有exclusions的配置
          .map(ConfigComparisonExclusionsConverter.INSTANCE::dtoFromDao)
          .collect(Collectors.toList());

      LOGGER.info("成功获取排除配置，appId: {}, operationId: {}, 配置数量: {}", appId, operationId, exclusionConfigs.size());
      return exclusionConfigs;

         } catch (Exception e) {
       LOGGER.error("查询排除配置异常，appId: {}, operationId: {}, error: {}", appId, operationId, e.getMessage(), e);
       return Collections.emptyList();
     }
   }

  /**
   * 从缓存获取排除配置
   * 
   * @param appId 应用ID
   * @return 排除配置列表，如果缓存未命中返回null
   */
  private List<ExclusionConfigItem> getFromCache(String appId) {
    try {
      String cacheKey = buildCacheKey(appId);
      byte[] json = redisCacheProvider.get(cacheKey.getBytes(StandardCharsets.UTF_8));
      if (json == null) {
        return null;
      }
      return objectMapper.readValue(json, new TypeReference<List<ExclusionConfigItem>>() {});
    } catch (Exception e) {
      LOGGER.warn("从缓存获取排除配置失败，appId: {}, error: {}", appId, e.getMessage());
      return null;
    }
  }

  /**
   * 将排除配置放入缓存
   * 
   * @param appId 应用ID
   * @param exclusionConfigs 排除配置列表
   */
  private void putToCache(String appId, List<ExclusionConfigItem> exclusionConfigs) {
    try {
      String cacheKey = buildCacheKey(appId);
      String jsonValue = JsonUtils.objectToJsonString(exclusionConfigs);
      redisCacheProvider.put(
          cacheKey.getBytes(StandardCharsets.UTF_8),
          CACHE_EXPIRE_SECONDS,
          jsonValue.getBytes(StandardCharsets.UTF_8)
      );
      LOGGER.debug("排除配置已缓存，appId: {}, 缓存时间: {}秒", appId, CACHE_EXPIRE_SECONDS);
    } catch (Exception e) {
      LOGGER.warn("缓存排除配置失败，appId: {}, error: {}", appId, e.getMessage());
    }
  }

  /**
   * 构建缓存键
   * 
   * @param appId 应用ID
   * @return 缓存键
   */
  private String buildCacheKey(String appId) {
    return EXCLUSION_CONFIG_CACHE_PREFIX + appId;
  }

  /**
   * 清除指定应用的排除配置缓存
   * 
   * @param appId 应用ID
   */
  public void clearCache(String appId) {
    try {
      String cacheKey = buildCacheKey(appId);
      redisCacheProvider.remove(cacheKey.getBytes(StandardCharsets.UTF_8));
      LOGGER.info("已清除排除配置缓存，appId: {}", appId);
    } catch (Exception e) {
      LOGGER.warn("清除排除配置缓存失败，appId: {}, error: {}", appId, e.getMessage());
    }
  }

  /**
   * 预加载排除配置到缓存（计划执行时调用）
   * 强制从数据库获取最新数据并更新缓存
   * 
   * @param replayPlan 回放计划
   */
  public void preloadExclusionConfig(ReplayPlan replayPlan) {
    if (replayPlan == null || replayPlan.getAppId() == null) {
      LOGGER.warn("跳过排除配置预加载，计划或应用ID为空");
      return;
    }

    try {
      String appId = replayPlan.getAppId();
      LOGGER.info("开始预加载排除配置，appId: {}, planId: {}", appId, replayPlan.getId());
      
      // 先清除现有缓存，确保获取最新数据
      clearCache(appId);
      
      // 重新查询并缓存（queryExclusionConfig会从数据库查询并缓存结果）
      List<ExclusionConfigItem> exclusionConfigs = queryExclusionConfig(appId);
      
      LOGGER.info("排除配置预加载完成，appId: {}, planId: {}, 配置数量: {}", 
          appId, replayPlan.getId(), exclusionConfigs.size());
      
    } catch (Exception e) {
      LOGGER.error("排除配置预加载失败，appId: {}, planId: {}, error: {}", 
          replayPlan.getAppId(), replayPlan.getId(), e.getMessage(), e);
    }
  }
} 