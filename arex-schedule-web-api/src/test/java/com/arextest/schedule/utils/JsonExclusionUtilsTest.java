package com.arextest.schedule.utils;

import com.arextest.schedule.model.exclusion.ExclusionConfigItem;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * JsonExclusionUtils单元测试
 */
public class JsonExclusionUtilsTest {

  private String testJson;
  private List<ExclusionConfigItem> exclusionConfigs;

  @BeforeEach
  void setUp() throws IOException {
    // 从文件读取测试JSON
    String resourcePath = "src/test/resources/data/json2.txt";
    testJson = new String(Files.readAllBytes(Paths.get(resourcePath)));

    // 创建排除配置：parameters.param1.bscExt 下任意层级的 unload_receipt_imgs
    ExclusionConfigItem config1 = new ExclusionConfigItem();
    config1.setExclusions(Arrays.asList("parameters", "param1", "bscExt", "..unload_receipt_imgs"));
    
    // 同时也要删除 parameters 解析后的 et.bscExt 下的 unload_receipt_imgs
    ExclusionConfigItem config2 = new ExclusionConfigItem();
    config2.setExclusions(Arrays.asList("parameters", "et", "bscExt", "..unload_receipt_imgs"));
    
    exclusionConfigs = Arrays.asList(config1, config2);
  }

  @Test
  void testApplyExclusions_ShouldRemoveNestedJsonStringField() {
    System.out.println("=== 测试开始 ===");
    System.out.println("排除配置1: " + exclusionConfigs.get(0).getExclusions());
    System.out.println("排除配置2: " + exclusionConfigs.get(1).getExclusions());
    
    // 执行排除操作
    String result = JsonExclusionUtils.applyExclusions(testJson, exclusionConfigs);
    
    System.out.println("=== 处理结果 ===");
    System.out.println(result);
    System.out.println("=== 结果结束 ===");
    
    System.out.println("是否包含unload_receipt_imgs: " + result.contains("unload_receipt_imgs"));
    
    // 验证结果
    assertNotNull(result);
    assertFalse(result.contains("unload_receipt_imgs"), 
        "应该移除嵌套JSON字符串中的unload_receipt_imgs字段");
    
    // 验证其他字段仍然存在
    assertTrue(result.contains("dbName"));
    assertTrue(result.contains("parameters"));
  }

  @Test
  void testApplyExclusions_WithEmptyConfig() {
    String result = JsonExclusionUtils.applyExclusions(testJson, Collections.emptyList());
    assertEquals(testJson, result, "空配置应该返回原始JSON");
  }

  @Test
  void testApplyExclusions_WithNullJson() {
    String result = JsonExclusionUtils.applyExclusions(null, exclusionConfigs);
    assertNull(result, "null输入应该返回null");
  }

  @Test
  void testApplyExclusions_WithInvalidJson() {
    String invalidJson = "invalid json";
    String result = JsonExclusionUtils.applyExclusions(invalidJson, exclusionConfigs);
    assertEquals(invalidJson, result, "无效JSON应该返回原始字符串");
  }

  @Test
  void testApplyExclusions_WithNoDotsInLastElement() {
    // 使用简单的JSON数据进行测试
    String simpleJson = "{\"field1\":\"value1\",\"field2\":\"value2\"}";
    
    // 创建不包含点号的配置
    ExclusionConfigItem config = new ExclusionConfigItem();
    config.setExclusions(Arrays.asList("field1"));  // 最后一个元素不包含点号
    List<ExclusionConfigItem> configs = Collections.singletonList(config);
    
    String result = JsonExclusionUtils.applyExclusions(simpleJson, configs);
    assertEquals(simpleJson, result, "最后一个元素不包含点号时应该跳过处理");
  }

  @Test
  void testApplyExclusions_WithDotsInLastElement() {
    // 使用简单的JSON数据进行测试
    String simpleJson = "{\"field1\":\"value1\",\"field2\":\"{\\\"nested\\\":\\\"value\\\"}\"}";
    
    // 创建包含点号的配置
    ExclusionConfigItem config = new ExclusionConfigItem();
    config.setExclusions(Arrays.asList("field2", ".nested"));  // 最后一个元素包含点号
    List<ExclusionConfigItem> configs = Collections.singletonList(config);
    
    String result = JsonExclusionUtils.applyExclusions(simpleJson, configs);
    assertNotEquals(simpleJson, result, "最后一个元素包含点号时应该进行处理");
    assertFalse(result.contains("nested"), "应该删除nested字段");
  }

  @Test
  public void testNestedStringExclusion() {
    // 测试您遇到的具体案例
    String jsonStr = "{\"126.661998,45.742253\":\"{\\\"arr_t\\\":\\\"2025-06-20 15:04:58\\\",\\\"arrival_user_role\\\":\\\"driver\\\",\\\"receipt_imgs\\\":[],\\\"unload_t\\\":\\\"2025-06-20 15:05:58\\\",\\\"unload_receipt_imgs\\\":[]}\"}";
    
    ExclusionConfigItem config = new ExclusionConfigItem();
    config.setExclusions(Arrays.asList("126.661998,45.742253", ".unload_receipt_imgs"));
    
    String result = JsonExclusionUtils.applyExclusions(jsonStr, Arrays.asList(config));
    
    System.out.println("原始JSON: " + jsonStr);
    System.out.println("处理后JSON: " + result);
    
    // 验证unload_receipt_imgs字段被删除
    assertFalse(result.contains("unload_receipt_imgs"));
  }

  @Test
  public void testWildcardExclusion() {
    String jsonStr = "{\"data\":\"{\\\"level1\\\":{\\\"level2\\\":{\\\"target_field\\\":\\\"value\\\"}},\\\"target_field\\\":\\\"top_value\\\"}\"}";
    
    ExclusionConfigItem config = new ExclusionConfigItem();
    config.setExclusions(Arrays.asList("data", "..target_field"));
    
    String result = JsonExclusionUtils.applyExclusions(jsonStr, Arrays.asList(config));
    
    System.out.println("通配符测试原始JSON: " + jsonStr);
    System.out.println("通配符测试处理后JSON: " + result);
    
    // 验证所有层级的target_field都被删除
    assertFalse(result.contains("target_field"));
  }

  @Test
  public void testCaseInsensitiveFieldMatching() {
    // 测试大小写不敏感的字段匹配
    String jsonStr = "{\"Parameters\":\"{\\\"Param1\\\":{\\\"BscExt\\\":{\\\"unload_receipt_imgs\\\":[1,2,3]}}}\"}";
    
    // 配置使用小写，但JSON中的key是大小写混合的
    ExclusionConfigItem config = new ExclusionConfigItem();
    config.setExclusions(Arrays.asList("parameters", "param1", "bscext", ".unload_receipt_imgs"));
    
    String result = JsonExclusionUtils.applyExclusions(jsonStr, Arrays.asList(config));
    
    System.out.println("大小写不敏感测试原始JSON: " + jsonStr);
    System.out.println("大小写不敏感测试处理后JSON: " + result);
    
    // 验证unload_receipt_imgs字段被删除（即使配置是小写，JSON key是大写）
    assertFalse(result.contains("unload_receipt_imgs"));
    // 验证其他字段仍然存在
    assertTrue(result.contains("Parameters"));
    assertTrue(result.contains("Param1"));
    assertTrue(result.contains("BscExt"));
  }

  @Test
  public void testCaseInsensitiveWildcardMatching() {
    // 测试大小写不敏感的通配符匹配
    String jsonStr = "{\"Data\":\"{\\\"Level1\\\":{\\\"Level2\\\":{\\\"Target_Field\\\":\\\"value\\\"}},\\\"Target_Field\\\":\\\"top_value\\\"}\"}";
    
    // 配置使用小写，但JSON中的key是大小写混合的
    ExclusionConfigItem config = new ExclusionConfigItem();
    config.setExclusions(Arrays.asList("data", "..target_field"));
    
    String result = JsonExclusionUtils.applyExclusions(jsonStr, Arrays.asList(config));
    
    System.out.println("大小写不敏感通配符测试原始JSON: " + jsonStr);
    System.out.println("大小写不敏感通配符测试处理后JSON: " + result);
    
    // 验证所有层级的Target_Field都被删除（即使配置是小写）
    assertFalse(result.contains("Target_Field"));
    // 验证其他字段仍然存在
    assertTrue(result.contains("Data"));
    assertTrue(result.contains("Level1"));
    assertTrue(result.contains("Level2"));
  }
} 