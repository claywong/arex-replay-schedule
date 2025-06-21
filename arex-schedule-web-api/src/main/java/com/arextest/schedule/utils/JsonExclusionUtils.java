package com.arextest.schedule.utils;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import com.alibaba.fastjson2.JSONArray;
import com.arextest.schedule.model.exclusion.ExclusionConfigItem;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

/**
 * JSON字段排除工具类 - 基于JSONPath实现
 * 支持路径数组格式的字段排除，如：["parameters", "param1", "et", "bscExt..unload_receipt_imgs"]
 * 表示在 parameters.param1.et.bscExt 路径下的任意层级移除 unload_receipt_imgs 字段
 */
@Slf4j
public class JsonExclusionUtils {

  /**
   * 根据排除配置处理JSON字符串，移除指定的字段
   * 
   * @param jsonStr JSON字符串
   * @param exclusionConfigs 排除配置列表
   * @return 处理后的JSON字符串
   */
  public static String applyExclusions(String jsonStr, List<ExclusionConfigItem> exclusionConfigs) {
    if (StringUtils.isBlank(jsonStr) || CollectionUtils.isEmpty(exclusionConfigs)) {
      return jsonStr;
    }

    try {
      Object jsonObj = JSON.parse(jsonStr);
      
      for (ExclusionConfigItem config : exclusionConfigs) {
        if (config != null && !CollectionUtils.isEmpty(config.getExclusions())) {
          processExclusionConfig(jsonObj, config.getExclusions());
        }
      }
      
      return JSON.toJSONString(jsonObj);
    } catch (Exception e) {
      LOGGER.warn("处理JSON排除配置时发生异常: {}", e.getMessage());
      return jsonStr;
    }
  }

  /**
   * 处理单个排除配置
   * 
   * @param jsonObj JSON对象
   * @param exclusions 排除路径数组
   */
  private static void processExclusionConfig(Object jsonObj, List<String> exclusions) {
    if (CollectionUtils.isEmpty(exclusions)) {
      return;
    }

    // 1. 检查最后一个元素是否包含 . 或 ..，如果不包含则跳过
    String lastElement = exclusions.get(exclusions.size() - 1);
    if (!lastElement.contains(".")) {
      LOGGER.debug("跳过不包含点号的排除路径: {}", lastElement);
      return;
    }

    try {
      // 2. 逐级遍历路径，处理每一级可能的字符串解析
      String fieldPattern = extractFieldPattern(lastElement);
      boolean isWildcard = lastElement.contains("..");
      
      // 构建到目标字段父级的路径
      List<String> targetPath = buildTargetPathList(exclusions);
      
      // 3. 逐级导航到目标位置并执行删除
      navigateAndDelete(jsonObj, targetPath, fieldPattern, isWildcard);

    } catch (Exception e) {
      LOGGER.warn("处理排除配置时发生异常: {}, 配置: {}", e.getMessage(), exclusions);
    }
  }

  /**
   * 导航到目标并执行删除操作
   * 
   * @param rootObj 根对象
   * @param pathList 路径列表
   * @param fieldPattern 字段匹配模式
   * @param isWildcard 是否为通配符模式
   */
  private static void navigateAndDelete(Object rootObj, List<String> pathList, String fieldPattern, boolean isWildcard) {
    if (pathList.isEmpty()) {
      // 如果路径为空，直接在根对象上删除
      deleteFieldsInTarget(rootObj, fieldPattern, isWildcard);
      return;
    }
    
    Object currentObj = rootObj;
    
    // 逐级导航到目标位置
    for (int i = 0; i < pathList.size(); i++) {
      String key = pathList.get(i);
      
      if (!(currentObj instanceof JSONObject)) {
        LOGGER.debug("当前对象不是JSONObject，无法继续导航: {}", currentObj.getClass());
        return;
      }
      
      JSONObject jsonObject = (JSONObject) currentObj;
      // 使用大小写不敏感的方式获取字段
      String actualKey = findKeyIgnoreCase(jsonObject, key);
      if (actualKey == null) {
        LOGGER.debug("路径不存在: {}", key);
        return;
      }
      
      Object nextObj = jsonObject.get(actualKey);
      
      if (nextObj == null) {
        LOGGER.debug("路径不存在: {}", key);
        return;
      }
      
      // 检查是否为字符串，需要解析
      if (nextObj instanceof String && isJsonString((String) nextObj)) {
        try {
          Object parsedObj = JSON.parse((String) nextObj);
          
          // 如果是最后一级路径
          if (i == pathList.size() - 1) {
            // 在解析后的对象中删除字段
            deleteFieldsInTarget(parsedObj, fieldPattern, isWildcard);
            // 将修改后的对象重新序列化并替换
            String modifiedStr = JSON.toJSONString(parsedObj);
            jsonObject.put(actualKey, modifiedStr);
            LOGGER.debug("更新最后一级字符串字段: {} = {}", actualKey, modifiedStr);
            return;
          } else {
            // 不是最后一级，需要继续导航
            // 递归处理剩余路径
            List<String> remainingPath = pathList.subList(i + 1, pathList.size());
            navigateAndDelete(parsedObj, remainingPath, fieldPattern, isWildcard);
            
            // 将修改后的对象重新序列化并替换回去
            String modifiedStr = JSON.toJSONString(parsedObj);
            jsonObject.put(actualKey, modifiedStr);
            LOGGER.debug("更新中间路径字符串字段: {} = {}", actualKey, modifiedStr);
            return;
          }
        } catch (Exception e) {
          LOGGER.debug("解析JSON字符串失败: {}", e.getMessage());
          return;
        }
      } else {
        // 如果是最后一级路径且不是字符串
        if (i == pathList.size() - 1) {
          // 如果是对象，直接删除字段
          deleteFieldsInTarget(nextObj, fieldPattern, isWildcard);
          return;
        } else {
          // 不是最后一级，继续导航
          currentObj = nextObj;
        }
      }
    }
  }

  /**
   * 构建目标路径列表（.. 或 . 之前的部分）
   * 
   * @param exclusions 排除路径数组
   * @return 目标路径列表
   */
  private static List<String> buildTargetPathList(List<String> exclusions) {
    List<String> pathList = new java.util.ArrayList<>();
    
    // 添加除最后一个元素外的所有路径部分
    for (int i = 0; i < exclusions.size() - 1; i++) {
      pathList.add(exclusions.get(i));
    }
    
    // 处理最后一个元素中 .. 或 . 之前的部分
    String lastElement = exclusions.get(exclusions.size() - 1);
    String[] parts;
    if (lastElement.contains("..")) {
      parts = lastElement.split("\\.\\.", 2);
    } else {
      parts = lastElement.split("\\.", 2);
    }
    
    if (parts.length >= 1 && StringUtils.isNotBlank(parts[0])) {
      pathList.add(parts[0]);
    }
    
    return pathList;
  }

  /**
   * 在目标对象中删除指定字段
   * 
   * @param targetObj 目标对象
   * @param fieldPattern 字段匹配模式
   * @param isWildcard 是否为通配符模式
   */
  private static void deleteFieldsInTarget(Object targetObj, String fieldPattern, boolean isWildcard) {
    if (isWildcard) {
      // 任意层级匹配：递归查找所有匹配的字段
      deleteFieldRecursively(targetObj, fieldPattern);
    } else {
      // 第一级别匹配：只检查直接子字段
      if (targetObj instanceof JSONObject) {
        JSONObject jsonObject = (JSONObject) targetObj;
        String actualKey = findKeyIgnoreCase(jsonObject, fieldPattern);
        if (actualKey != null) {
          jsonObject.remove(actualKey);
          LOGGER.debug("删除第一级字段: {} (实际key: {})", fieldPattern, actualKey);
        }
      }
    }
  }

  /**
   * 递归删除指定字段
   * 
   * @param obj 当前对象
   * @param targetField 目标字段名
   */
  private static void deleteFieldRecursively(Object obj, String targetField) {
    if (obj instanceof JSONObject) {
      JSONObject jsonObject = (JSONObject) obj;
      
      // 删除当前层级的目标字段（大小写不敏感）
      String actualKey = findKeyIgnoreCase(jsonObject, targetField);
      if (actualKey != null) {
        jsonObject.remove(actualKey);
        LOGGER.debug("递归删除字段: {} (实际key: {})", targetField, actualKey);
      }
      
      // 递归处理所有子对象
      for (Object value : jsonObject.values()) {
        deleteFieldRecursively(value, targetField);
      }
    } else if (obj instanceof JSONArray) {
      JSONArray jsonArray = (JSONArray) obj;
      for (Object item : jsonArray) {
        deleteFieldRecursively(item, targetField);
      }
    }
  }

  /**
   * 大小写不敏感地查找JSONObject中的key
   * 
   * @param jsonObject JSON对象
   * @param targetKey 目标key（小写配置）
   * @return 实际存在的key，如果不存在则返回null
   */
  private static String findKeyIgnoreCase(JSONObject jsonObject, String targetKey) {
    if (jsonObject == null || StringUtils.isBlank(targetKey)) {
      return null;
    }
    
    // 首先尝试精确匹配
    if (jsonObject.containsKey(targetKey)) {
      return targetKey;
    }
    
    // 然后进行大小写不敏感匹配
    for (String key : jsonObject.keySet()) {
      if (key != null && key.equalsIgnoreCase(targetKey)) {
        return key;
      }
    }
    
    return null;
  }

  /**
   * 检查字符串是否为有效的JSON格式
   * 
   * @param str 待检查的字符串
   * @return true如果是有效JSON，false否则
   */
  private static boolean isJsonString(String str) {
    if (StringUtils.isBlank(str)) {
      return false;
    }
    
    str = str.trim();
    
    // 检查是否以JSON对象或数组的格式开始和结束
    if ((str.startsWith("{") && str.endsWith("}")) || 
        (str.startsWith("[") && str.endsWith("]"))) {
      try {
        JSON.parse(str);
        return true;
      } catch (Exception e) {
        return false;
      }
    }
    
    return false;
  }

  /**
   * 提取字段匹配模式（.. 或 . 之后的部分）
   * 
   * @param lastElement 最后一个元素
   * @return 字段匹配模式
   */
  private static String extractFieldPattern(String lastElement) {
    String[] parts;
    if (lastElement.contains("..")) {
      parts = lastElement.split("\\.\\.", 2);
    } else {
      parts = lastElement.split("\\.", 2);
    }
    
    return parts.length >= 2 ? parts[1] : "";
  }
} 