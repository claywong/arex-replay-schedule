package com.arextest.schedule.model.dao.mongodb;

import java.util.Date;
import java.util.List;
import lombok.Data;
import lombok.experimental.FieldNameConstants;
import org.springframework.data.mongodb.core.mapping.Document;

/**
 * ConfigComparisonExclusions集合模型类
 */
@Data
@FieldNameConstants
@Document(collection = "ConfigComparisonExclusions")
public class ConfigComparisonExclusionsCollection extends ModelBase {

  /**
   * 应用ID
   */
  private String appId;

  /**
   * 比较配置类型
   */
  private int compareConfigType;

  /**
   * 依赖ID
   */
  private String dependencyId;

  /**
   * 排除字段列表
   */
  private List<String> exclusions;

  /**
   * 过期时间
   */
  private long expirationDate;

  /**
   * 过期类型
   */
  private int expirationType;

  /**
   * FS接口ID
   */
  private String fsInterfaceId;

  /**
   * 操作ID
   */
  private String operationId;
} 