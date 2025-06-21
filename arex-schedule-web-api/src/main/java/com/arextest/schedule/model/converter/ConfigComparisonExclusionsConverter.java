package com.arextest.schedule.model.converter;

import com.arextest.schedule.model.dao.mongodb.ConfigComparisonExclusionsCollection;
import com.arextest.schedule.model.exclusion.ExclusionConfigItem;
import org.mapstruct.Mapper;
import org.mapstruct.factory.Mappers;

/**
 * 排除配置转换器
 */
@Mapper
public interface ConfigComparisonExclusionsConverter {

  ConfigComparisonExclusionsConverter INSTANCE = Mappers.getMapper(ConfigComparisonExclusionsConverter.class);

  /**
   * 将MongoDB模型转换为业务模型
   */
  ExclusionConfigItem dtoFromDao(ConfigComparisonExclusionsCollection dao);

  /**
   * 将业务模型转换为MongoDB模型
   */
  ConfigComparisonExclusionsCollection daoFromDto(ExclusionConfigItem dto);
} 