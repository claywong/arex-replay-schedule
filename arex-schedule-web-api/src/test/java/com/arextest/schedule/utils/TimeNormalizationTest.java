package com.arextest.schedule.utils;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import java.lang.reflect.Method;
import com.arextest.schedule.comparer.impl.DefaultReplayResultComparer;

/**
 * 时间标准化功能测试
 */
public class TimeNormalizationTest {

  @Test
  public void testNestedJsonTimeNormalization() throws Exception {
    // 创建DefaultReplayResultComparer实例用于测试
    DefaultReplayResultComparer comparer = DefaultReplayResultComparer.builder()
        .compareConfigService(null)
        .sourceRemoteLoader(null)
        .progressTracer(null)
        .comparisonOutputWriter(null)
        .caseItemRepository(null)
        .metricService(null)
        .configHandler(null)
        .compareService(null)
        .exclusionConfigService(null)
        .build();
    
    // 使用反射访问private方法
    Method normalizeMethod = DefaultReplayResultComparer.class
        .getDeclaredMethod("normalizeTimeFieldsInJson", String.class);
    normalizeMethod.setAccessible(true);
    
    Method isValidJsonMethod = DefaultReplayResultComparer.class
        .getDeclaredMethod("isValidJsonString", String.class);
    isValidJsonMethod.setAccessible(true);
    
    // 测试1：简单的时间字段
    String simpleJson = "{\"timestamp\":\"2025-06-20 15:04:58\",\"data\":\"test\"}";
    String result1 = (String) normalizeMethod.invoke(comparer, simpleJson);
    System.out.println("Simple JSON result: " + result1);
    
    // 测试2：一层嵌套的JSON字符串（常见情况）
    String nestedJson = "{\"parameters\":\"{\\\"timestamp\\\":\\\"2025-06-20 15:04:58\\\",\\\"data\\\":\\\"test\\\"}\"}";
    String result2 = (String) normalizeMethod.invoke(comparer, nestedJson);
    System.out.println("Nested JSON result: " + result2);
    
    // 测试3：更实际的业务场景 - 类似您遇到的坐标数据
    String businessJson = "{\"126.661998,45.742253\":\"{\\\"arr_t\\\":\\\"2025-06-20 15:04:58\\\",\\\"arrival_user_role\\\":\\\"driver\\\",\\\"receipt_imgs\\\":[],\\\"unload_t\\\":\\\"2025-06-20 15:05:58\\\"}\"}";
    String result3 = (String) normalizeMethod.invoke(comparer, businessJson);
    System.out.println("Business JSON result: " + result3);
    
    // 测试4：数组中的嵌套JSON
    String arrayJson = "[{\"data\":\"{\\\"timestamp\\\":\\\"2025-06-20 15:04:58\\\"}\"}]";
    String result4 = (String) normalizeMethod.invoke(comparer, arrayJson);
    System.out.println("Array JSON result: " + result4);
    
    // 测试5：时间戳格式
    String timestampJson = "{\"ts\":\"1719037498000\",\"data\":\"test\"}";
    String result5 = (String) normalizeMethod.invoke(comparer, timestampJson);
    System.out.println("Timestamp JSON result: " + result5);
    
    // 验证结果
    assertNotNull(result1);
    assertNotNull(result2);
    assertNotNull(result3);
    assertNotNull(result4);
    assertNotNull(result5);
    
    // 验证时间被标准化了
    assertTrue(result1.contains("15:04:56"), "Simple JSON time should be normalized");
    assertTrue(result2.contains("15:04:56"), "Nested JSON time should be normalized");
    assertTrue(result3.contains("15:04:56") && result3.contains("15:05:56"), 
        "Business JSON times should be normalized");
    assertTrue(result4.contains("15:04:56"), "Array JSON time should be normalized");
    
    System.out.println("=== 所有测试完成 ===");
  }
} 