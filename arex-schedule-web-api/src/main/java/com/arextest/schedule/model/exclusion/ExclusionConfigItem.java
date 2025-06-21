package com.arextest.schedule.model.exclusion;

import java.util.List;
import lombok.Data;

/**
 * 排除配置项，对应API返回的body中的单个配置
 */
@Data
public class ExclusionConfigItem {
  
  /**
   * 状态
   */
  private String status;
  
  /**
   * 修改时间
   */
  private long modifiedTime;
  
  /**
   * 配置ID
   */
  private String id;
  
  /**
   * 应用ID
   */
  private String appId;
  
  /**
   * 操作ID
   */
  private String operationId;
  
  /**
   * 过期类型
   */
  private int expirationType;
  
  /**
   * 过期时间
   */
  private long expirationDate;
  
  /**
   * 比较配置类型
   */
  private int compareConfigType;
  
  /**
   * FS接口ID
   */
  private String fsInterfaceId;
  
  /**
   * 依赖ID
   */
  private String dependencyId;
  
  /**
   * 操作类型
   */
  private String operationType;
  
  /**
   * 操作名称
   */
  private String operationName;
  
  /**
   * 排除字段列表
   */
  private List<String> exclusions;
} 