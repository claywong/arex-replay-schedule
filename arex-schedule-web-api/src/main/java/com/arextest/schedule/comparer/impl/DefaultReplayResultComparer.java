package com.arextest.schedule.comparer.impl;

import com.alibaba.fastjson2.JSONObject;
import com.alibaba.fastjson2.JSONArray;
import com.arextest.diff.model.CompareOptions;
import com.arextest.diff.model.CompareResult;
import com.arextest.diff.model.enumeration.DiffResultCode;
import com.arextest.diff.sdk.CompareSDK;
import com.arextest.model.mock.MockCategoryType;
import com.arextest.schedule.comparer.CategoryComparisonHolder;
import com.arextest.schedule.comparer.CategoryComparisonHolder.CompareResultItem;
import com.arextest.schedule.comparer.CompareConfigService;
import com.arextest.schedule.comparer.CompareItem;
import com.arextest.schedule.comparer.CompareService;
import com.arextest.schedule.comparer.ComparisonWriter;
import com.arextest.schedule.comparer.CustomComparisonConfigurationHandler;
import com.arextest.schedule.comparer.EncodingUtils;
import com.arextest.schedule.comparer.ReplayResultComparer;
import com.arextest.schedule.dao.mongodb.ReplayActionCaseItemRepository;
import com.arextest.schedule.mdc.MDCTracer;
import com.arextest.schedule.model.CaseSendStatusType;
import com.arextest.schedule.model.CompareModeType;
import com.arextest.schedule.model.CompareProcessStatusType;
import com.arextest.schedule.model.LogType;
import com.arextest.schedule.model.ReplayActionCaseItem;
import com.arextest.schedule.model.ReplayCompareResult;
import com.arextest.schedule.model.config.ComparisonInterfaceConfig;
import com.arextest.schedule.model.config.ReplayComparisonConfig;
import com.arextest.schedule.progress.ProgressTracer;
import com.arextest.schedule.service.MetricService;
import com.arextest.schedule.service.ExclusionConfigService;
import com.arextest.schedule.model.exclusion.ExclusionConfigItem;
import com.arextest.schedule.utils.JsonExclusionUtils;
import com.arextest.web.model.contract.contracts.compare.CategoryDetail;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import com.arextest.web.model.contract.contracts.config.SystemConfigWithProperties;

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.util.StopWatch;

import java.text.SimpleDateFormat;
import java.text.ParseException;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

@Slf4j
@Builder
public class DefaultReplayResultComparer implements ReplayResultComparer {

  private static final long MAX_TIME = Long.MAX_VALUE;

  // 时间格式的正则表达式模式
  private static final Pattern TIME_PATTERN = Pattern.compile(
      "\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}");

  // 时间戳格式的正则表达式模式（10位秒级时间戳或13位毫秒级时间戳）
  private static final Pattern TIMESTAMP_PATTERN = Pattern.compile(
      "^1\\d{9,12}$");

  // 默认时间精度容忍度（毫秒）- 可以通过配置获取
  private static final long DEFAULT_TIME_TOLERANCE_MS = 4000;

  private final CompareConfigService compareConfigService;
  private final PrepareCompareSourceRemoteLoader sourceRemoteLoader;
  private final ProgressTracer progressTracer;
  private final ComparisonWriter comparisonOutputWriter;
  private final ReplayActionCaseItemRepository caseItemRepository;
  private final MetricService metricService;
  private final CustomComparisonConfigurationHandler configHandler;
  private final CompareService compareService;
  private final ExclusionConfigService exclusionConfigService;

  public DefaultReplayResultComparer(CompareConfigService compareConfigService,
      PrepareCompareSourceRemoteLoader sourceRemoteLoader,
      ProgressTracer progressTracer,
      ComparisonWriter comparisonOutputWriter,
      ReplayActionCaseItemRepository caseItemRepository,
      MetricService metricService,
      CustomComparisonConfigurationHandler configHandler,
      CompareService compareService,
      ExclusionConfigService exclusionConfigService) {
    this.compareConfigService = compareConfigService;
    this.sourceRemoteLoader = sourceRemoteLoader;
    this.progressTracer = progressTracer;
    this.comparisonOutputWriter = comparisonOutputWriter;
    this.caseItemRepository = caseItemRepository;
    this.metricService = metricService;
    this.configHandler = configHandler;
    this.compareService = compareService;
    this.exclusionConfigService = exclusionConfigService;
  }

  @Override
  public boolean compare(ReplayActionCaseItem caseItem, boolean useReplayId) {
    StopWatch compareWatch = new StopWatch();
    compareWatch.start(LogType.COMPARE.getValue());
    String planId = caseItem.getParent().getPlanId();
    try {
      MDCTracer.addPlanId(planId);
      MDCTracer.addPlanItemId(caseItem.getPlanItemId());

      List<CategoryComparisonHolder> waitCompareMap = sourceRemoteLoader.buildWaitCompareList(caseItem, useReplayId);
      if (CollectionUtils.isEmpty(waitCompareMap)) {
        caseItemRepository.updateCompareStatus(caseItem.getId(),
            CompareProcessStatusType.ERROR.getValue());
        caseItem.setCompareStatus(CompareProcessStatusType.ERROR.getValue());
        comparisonOutputWriter.writeIncomparable(caseItem,
            CaseSendStatusType.REPLAY_RESULT_NOT_FOUND.name());
        return true;
      }

      List<ReplayCompareResult> replayCompareResults = this.doContentCompare(caseItem,
          waitCompareMap);

      if (CollectionUtils.isEmpty(replayCompareResults)
          && MockCategoryType.Q_MESSAGE_CONSUMER.getName()
              .equalsIgnoreCase(caseItem.getCaseType())) {
        caseItemRepository.updateCompareStatus(caseItem.getId(),
            CompareProcessStatusType.PASS.getValue());
        caseItem.setCompareStatus(CompareProcessStatusType.PASS.getValue());
        return comparisonOutputWriter.writeQmqCompareResult(caseItem);
      }

      CompareProcessStatusType compareStatus = CompareProcessStatusType.PASS;
      for (ReplayCompareResult replayCompareResult : replayCompareResults) {
        if (replayCompareResult.getDiffResultCode() == DiffResultCode.COMPARED_WITH_DIFFERENCE) {
          compareStatus = CompareProcessStatusType.HAS_DIFF;
          break;
        } else if (replayCompareResult.getDiffResultCode() == DiffResultCode.COMPARED_INTERNAL_EXCEPTION) {
          compareStatus = CompareProcessStatusType.ERROR;
          break;
        }
      }
      caseItemRepository.updateCompareStatus(caseItem.getId(), compareStatus.getValue());
      caseItem.setCompareStatus(compareStatus.getValue());
      return comparisonOutputWriter.write(replayCompareResults);
    } catch (Throwable throwable) {
      caseItemRepository.updateCompareStatus(caseItem.getId(),
          CompareProcessStatusType.ERROR.getValue());
      caseItem.setCompareStatus(CompareProcessStatusType.ERROR.getValue());
      comparisonOutputWriter.writeIncomparable(caseItem, throwable.getMessage());
      LOGGER.error("compare case result error:{} ,case item: {}", throwable.getMessage(), caseItem,
          throwable);
      MDCTracer.clear();
      // don't send again
      return true;
    } finally {
      progressTracer.finishOne(caseItem);
      compareWatch.stop();
      metricService.recordTimeEvent(LogType.COMPARE.getValue(), planId,
          caseItem.getParent().getAppId(), null,
          compareWatch.getTotalTimeMillis());
      long caseExecutionEndMills = System.currentTimeMillis();
      metricService.recordTimeEvent(LogType.CASE_EXECUTION_TIME.getValue(), planId,
          caseItem.getParent().getAppId(), null,
          caseExecutionEndMills - caseItem.getExecutionStartMillis());
      MDCTracer.clear();
    }
  }

  @Override
  public List<ReplayCompareResult> doContentCompare(ReplayActionCaseItem caseItem,
      List<CategoryComparisonHolder> waitCompareMap) {
    ComparisonInterfaceConfig operationConfig = compareConfigService.loadInterfaceConfig(
        caseItem.getParent());

    List<ReplayCompareResult> replayCompareResults = new ArrayList<>();
    for (CategoryComparisonHolder bindHolder : waitCompareMap) {
      if (operationConfig.checkIgnoreMockMessageType(bindHolder.getCategoryName())) {
        continue;
      }
      replayCompareResults.addAll(compareReplayResult(bindHolder, caseItem, operationConfig));
    }
    return replayCompareResults;
  }

  /**
   * compare recording and replay data.
   */
  private List<ReplayCompareResult> compareReplayResult(CategoryComparisonHolder bindHolder,
      ReplayActionCaseItem caseItem, ComparisonInterfaceConfig operationConfig) {
    if (Boolean.TRUE.equals(bindHolder.getNeedMatch())) {
      return matchCompareReplayResults(bindHolder, caseItem, operationConfig);
    }

    return getReplayCompareResults(bindHolder, caseItem, operationConfig);
  }

  private List<ReplayCompareResult> getReplayCompareResults(CategoryComparisonHolder bindHolder,
      ReplayActionCaseItem caseItem, ComparisonInterfaceConfig operationConfig) {
    CompareResultItem item = bindHolder.getCompareResultItem();
    if (item == null) {
      return Collections.emptyList();
    }

    List<ReplayCompareResult> compareResults = new ArrayList<>();
    compareResults.add(compareRecordAndResult(operationConfig, caseItem, bindHolder.getCategoryName(),
        item.getReplayItem(), item.getRecordItem()));
    return compareResults;
  }

  /**
   * record and replay data through compareKey.
   * 
   * @param bindHolder
   * @param caseItem
   * @param operationConfig
   * @return
   */
  private List<ReplayCompareResult> matchCompareReplayResults(CategoryComparisonHolder bindHolder,
      ReplayActionCaseItem caseItem, ComparisonInterfaceConfig operationConfig) {
    List<ReplayCompareResult> compareResults = new ArrayList<>();
    List<CompareItem> recordResults = bindHolder.getRecord();
    List<CompareItem> replayResults = bindHolder.getReplayResult();

    boolean sourceEmpty = CollectionUtils.isEmpty(recordResults);
    boolean targetEmpty = CollectionUtils.isEmpty(replayResults);
    if (sourceEmpty && targetEmpty) {
      return Collections.emptyList();
    }
    final String category = bindHolder.getCategoryName();
    if (sourceEmpty) {
      replayResults.forEach(replayResult -> {
        compareResults.add(
            compareRecordAndResult(operationConfig, caseItem, category, replayResult, null));
      });
      return compareResults;
    }
    if (targetEmpty) {
      recordResults.forEach(recordResult -> {
        compareResults.add(
            compareRecordAndResult(operationConfig, caseItem, category, null, recordResult));
      });
      return compareResults;
    }

    Map<String, List<CompareItem>> recordMap = recordResults.stream()
        .filter(data -> StringUtils.isNotEmpty(data.getCompareKey()))
        .collect(Collectors.groupingBy(CompareItem::getCompareKey));

    Set<String> usedRecordKeys = new HashSet<>();
    for (CompareItem resultCompareItem : replayResults) {
      // config for operation if its entrypoint, dependency config otherwise
      String compareKey = resultCompareItem.getCompareKey();

      if (resultCompareItem.isEntryPointCategory()) {
        compareResults.add(
            compareRecordAndResult(operationConfig, caseItem, category, resultCompareItem,
                recordResults.get(0)));
        return compareResults;
      }

      if (StringUtils.isEmpty(compareKey)) {
        compareResults.add(
            compareRecordAndResult(operationConfig, caseItem, category, resultCompareItem, null));
        continue;
      }

      if (recordMap.containsKey(compareKey)) {
        List<CompareItem> recordCompareItems = recordMap.get(compareKey);
        if (CollectionUtils.isEmpty(recordCompareItems)) {
          continue;
        }
        compareResults.add(
            compareRecordAndResult(operationConfig, caseItem, category, resultCompareItem,
                recordCompareItems.get(0)));
        usedRecordKeys.add(compareKey);
      } else {
        compareResults.add(
            compareRecordAndResult(operationConfig, caseItem, category, resultCompareItem, null));
      }
    }

    recordMap.keySet().stream().filter(key -> !usedRecordKeys.contains(key)) // unused keys
        .forEach(key -> {
          recordMap.get(key).forEach(recordItem -> {
            compareResults.add(
                compareRecordAndResult(operationConfig, caseItem, category, null, recordItem));
          });
        });
    return compareResults;
  }

  private ReplayCompareResult compareRecordAndResult(ComparisonInterfaceConfig operationConfig,
      ReplayActionCaseItem caseItem, String category, CompareItem target, CompareItem source) {

    String operation = source != null ? source.getCompareOperation() : target.getCompareOperation();
    String record = source != null ? source.getCompareContent() : null;
    String replay = target != null ? target.getCompareContent() : null;

    ReplayComparisonConfig compareConfig = configHandler.pickConfig(operationConfig, category,
        operation);

    CompareResult comparedResult = new CompareResult();
    ReplayCompareResult resultNew = ReplayCompareResult.createFrom(caseItem);

    // use operation config to ignore category
    if (ignoreCategory(category, operation, operationConfig.getIgnoreCategoryTypes())) {
      comparedResult.setCode(DiffResultCode.COMPARED_WITHOUT_DIFFERENCE);
      comparedResult.setProcessedBaseMsg(record);
      comparedResult.setProcessedTestMsg(replay);
      mergeResult(operation, category, resultNew, comparedResult, source, target);
      resultNew.setIgnore(true);
      return resultNew;
    }

    StopWatch stopWatch = new StopWatch();
    stopWatch.start(LogType.COMPARE_SDK.getValue());
    comparedResult = compareProcess(category, record, replay, compareConfig,
        caseItem.getCompareMode().getValue(), caseItem.getParent().getAppId());
    stopWatch.stop();

    // new call & call missing don't record time
    if (target != null && source != null) {
      metricService.recordTimeEvent(LogType.COMPARE_SDK.getValue(),
          caseItem.getParent().getPlanId(),
          caseItem.getParent().getAppId(), source.getCompareContent(),
          stopWatch.getTotalTimeMillis());
    }

    mergeResult(operation, category, resultNew, comparedResult, source, target);
    return resultNew;
  }

  /**
   * 从URL字符串中移除指定的参数及其值
   *
   * @param url            原始的URL字符串
   * @param paramsToRemove 要移除的参数名
   * @return 处理后的URL字符串
   */
  public String removeParams(String url, String... paramsToRemove) {
    try {
      String modifiedUrl = url;
      for (String param : paramsToRemove) {
        modifiedUrl = modifiedUrl.replaceAll("(&?)" + param + "=[^&]*", "");
      }
      // 处理可能出现的多余的&&情况
      modifiedUrl = modifiedUrl.replaceAll("&&", "&");
      // 处理可能出现的末尾&情况
      modifiedUrl = modifiedUrl.endsWith("&") ? modifiedUrl.substring(0, modifiedUrl.length() - 1) : modifiedUrl;
      // 处理可能出现的开头&情况
      modifiedUrl = modifiedUrl.startsWith("&") ? modifiedUrl.substring(1) : modifiedUrl;
      return modifiedUrl;
    } catch (Exception e) {
      return url;
    }
  }

  /**
   * 将queryString解析成json格式
   *
   * @param queryString
   * @return
   */
  public String queryStringToJson(String queryString) {
    try {
      queryString = removeParams(queryString, "g7timestamp", "sign", "accessid");
      if (!queryString.contains("&")) {
        return queryString;
      }
      JSONObject json = new JSONObject();
      // 使用&分割字符串
      String[] pairs = queryString.split("&");
      for (String pair : pairs) {
        if (!pair.contains("=")) {
          // 处理不含等号的键，这里默认值为空字符串
          json.put(pair, "");
        } else {
          // 将键值对分割，并存入JSONObject
          String[] keyValue = pair.split("=", 2);
          json.put(keyValue[0], keyValue[1]);
        }
      }
      return json.toJSONString();
    } catch (Exception e) {
      return queryString;
    }
  }

  private String filterCustomReq(String string) {
    if (string == null) {
      return null;
    }
    try {
      String decodedStr = new String(Base64.getDecoder().decode(string), StandardCharsets.UTF_8);
      if (decodedStr.startsWith("req=")) {
        // 过滤掉req=
        decodedStr = URLDecoder.decode(decodedStr.substring(4), "UTF-8");
        return decodedStr;
      }
    } catch (Exception ignored) {
      // Exception handling if required
    }
    return string;
  }

  private CompareResult compareProcess(String category, String record, String result,
      ReplayComparisonConfig compareConfig, int compareMode, String appId) {
    CompareOptions options = configHandler.buildSkdOption(category, compareConfig);
    try {
      // to-do: 64base extract record and result
      String decodedRecord = EncodingUtils.tryBase64Decode(record);
      String decodedResult = EncodingUtils.tryBase64Decode(result);

      // 如果是GET格式，则去掉其中的g7timestamp和sign
      // extend&gpsnos=71032425&map&accessid=wxaaebgby9ndwf6dm&g7timestamp=1706600220689&sign=Si2Nd2Gmcc/vhTAKjJQ1AFp5QeU=
      // 代码转换成JSON
      if (decodedResult != null && !EncodingUtils.isJson(decodedResult)) {
        decodedResult = queryStringToJson(decodedResult);
        decodedRecord = queryStringToJson(decodedRecord);
      }

      decodedResult = filterCustomReq(decodedResult);
      decodedRecord = filterCustomReq(decodedRecord);

      // 新增：对JSON字符串中的时间字段进行标准化处理
      if (decodedRecord != null && EncodingUtils.isJson(decodedRecord)) {
//        LOGGER.debug("Before normalization - record: {}", decodedRecord);
        decodedRecord = normalizeTimeFieldsInJson(decodedRecord);
//        LOGGER.debug("After normalization - record: {}", decodedRecord);
      }
      if (decodedResult != null && EncodingUtils.isJson(decodedResult)) {
//        LOGGER.debug("Before normalization - result: {}", decodedResult);
        decodedResult = normalizeTimeFieldsInJson(decodedResult);
//        LOGGER.debug("After normalization - result: {}", decodedResult);
      }

      // 新增：应用排除配置，移除需要忽略的字段
      if (appId != null && (EncodingUtils.isJson(decodedRecord) || EncodingUtils.isJson(decodedResult))) {
        List<ExclusionConfigItem> exclusionConfigs = exclusionConfigService.queryExclusionConfig(appId);
        if (!exclusionConfigs.isEmpty()) {
          // 应用排除配置到 record
          if (decodedRecord != null && EncodingUtils.isJson(decodedRecord) && decodedRecord.contains("unload_receipt_imgs") ) {
            LOGGER.debug("应用JSON字段排除前 - record: {}", decodedRecord);
            decodedRecord = JsonExclusionUtils.applyExclusions(decodedRecord, exclusionConfigs);
            LOGGER.debug("应用JSON字段排除后 - record: {}", decodedRecord);
          }
          // 应用排除配置到 result
          if (decodedResult != null && EncodingUtils.isJson(decodedResult) && decodedResult.contains("unload_receipt_imgs") ) {
            LOGGER.debug("应用JSON字段排除前 - result: {}", decodedResult);
            decodedResult = JsonExclusionUtils.applyExclusions(decodedResult, exclusionConfigs);
            LOGGER.debug("应用JSON字段排除后 - result: {}", decodedResult);
          }
        }
      }

      if (compareMode == CompareModeType.FULL.getValue()) {
        return compareService.compare(decodedRecord, decodedResult, options);
      }
      return compareService.quickCompare(decodedRecord, decodedResult, options);

    } catch (Throwable e) {
      LOGGER.error("run compare sdk process error:{} ,source: {} ,target:{}", e.getMessage(),
          record, result);
      return CompareSDK.fromException(record, result, e.getMessage());
    }
  }

  /**
   * 对JSON字符串中的时间字段进行标准化处理
   * 如果时间差异在容忍范围内，则统一为同一个时间值
   *
   * @param jsonStr JSON字符串
   * @return 处理后的JSON字符串
   */
  private String normalizeTimeFieldsInJson(String jsonStr) {
    try {
      if (jsonStr.startsWith("{") && jsonStr.endsWith("}")) {
        JSONObject jsonObject = JSONObject.parseObject(jsonStr);
        normalizeTimeFieldsInJsonObject(jsonObject);
        return jsonObject.toJSONString();
      } else if (jsonStr.startsWith("[") && jsonStr.endsWith("]")) {
        JSONArray jsonArray = JSONArray.parseArray(jsonStr);
        normalizeTimeFieldsInJsonArray(jsonArray);
        return jsonArray.toJSONString();
      }
    } catch (Exception e) {
      LOGGER.debug("Failed to normalize time fields in JSON: {}, error: {}", jsonStr, e.getMessage());
    }
    return jsonStr;
  }

  /**
   * 递归处理JSONObject中的时间字段
   */
  private void normalizeTimeFieldsInJsonObject(JSONObject jsonObject) {
    if (jsonObject == null) {
      return;
    }

    for (String key : jsonObject.keySet()) {
      Object value = jsonObject.get(key);
      if (value instanceof String) {
        String strValue = (String) value;
        if (isTimeFormat(strValue)) {
          // 对时间字符串进行标准化处理
          String normalizedTime = normalizeTimeString(strValue);
          jsonObject.put(key, normalizedTime);
        } else if (isTimestamp(strValue)) {
          // 对时间戳进行标准化处理
          String normalizedTime = normalizeTimestamp(strValue);
          jsonObject.put(key, normalizedTime);
        } else if (isValidJsonString(strValue)) {
          // 处理嵌套的JSON字符串 - 采用逐级解析和替换的方法
          try {
            Object parsedObj = com.alibaba.fastjson2.JSON.parse(strValue);
            if (parsedObj instanceof JSONObject) {
              normalizeTimeFieldsInJsonObject((JSONObject) parsedObj);
            } else if (parsedObj instanceof JSONArray) {
              normalizeTimeFieldsInJsonArray((JSONArray) parsedObj);
            }
            // 将修改后的对象重新序列化并替换
            String modifiedStr = com.alibaba.fastjson2.JSON.toJSONString(parsedObj);
            jsonObject.put(key, modifiedStr);
          } catch (Exception e) {
            LOGGER.debug("Failed to parse nested JSON string: {}, error: {}", strValue, e.getMessage());
          }
        }
      } else if (value instanceof Number) {
        // 处理数字类型的时间戳
        Number numValue = (Number) value;
        if (isTimestamp(numValue.toString())) {
          String normalizedTime = normalizeTimestamp(numValue.toString());
          jsonObject.put(key, normalizedTime);
        }
      } else if (value instanceof JSONObject) {
        normalizeTimeFieldsInJsonObject((JSONObject) value);
      } else if (value instanceof JSONArray) {
        normalizeTimeFieldsInJsonArray((JSONArray) value);
      }
    }
  }

  /**
   * 递归处理JSONArray中的时间字段
   */
  private void normalizeTimeFieldsInJsonArray(JSONArray jsonArray) {
    if (jsonArray == null) {
      return;
    }

    for (int i = 0; i < jsonArray.size(); i++) {
      Object value = jsonArray.get(i);
      if (value instanceof String) {
        String strValue = (String) value;
        if (isTimeFormat(strValue)) {
          String normalizedTime = normalizeTimeString(strValue);
          jsonArray.set(i, normalizedTime);
        } else if (isTimestamp(strValue)) {
          String normalizedTime = normalizeTimestamp(strValue);
          jsonArray.set(i, normalizedTime);
        } else if (isValidJsonString(strValue)) {
          // 处理嵌套的JSON字符串 - 采用逐级解析和替换的方法
          try {
            Object parsedObj = com.alibaba.fastjson2.JSON.parse(strValue);
            if (parsedObj instanceof JSONObject) {
              normalizeTimeFieldsInJsonObject((JSONObject) parsedObj);
            } else if (parsedObj instanceof JSONArray) {
              normalizeTimeFieldsInJsonArray((JSONArray) parsedObj);
            }
            // 将修改后的对象重新序列化并替换
            String modifiedStr = com.alibaba.fastjson2.JSON.toJSONString(parsedObj);
            jsonArray.set(i, modifiedStr);
          } catch (Exception e) {
            LOGGER.debug("Failed to parse nested JSON string: {}, error: {}", strValue, e.getMessage());
          }
        }
      } else if (value instanceof Number) {
        Number numValue = (Number) value;
        if (isTimestamp(numValue.toString())) {
          String normalizedTime = normalizeTimestamp(numValue.toString());
          jsonArray.set(i, normalizedTime);
        }
      } else if (value instanceof JSONObject) {
        normalizeTimeFieldsInJsonObject((JSONObject) value);
      } else if (value instanceof JSONArray) {
        normalizeTimeFieldsInJsonArray((JSONArray) value);
      }
    }
  }

  /**
   * 判断字符串是否为时间格式
   */
  private boolean isTimeFormat(String str) {
    if (str == null || str.isEmpty()) {
      return false;
    }
    return TIME_PATTERN.matcher(str).matches();
  }

  /**
   * 判断字符串是否为时间戳格式（10位秒级或13位毫秒级）
   */
  private boolean isTimestamp(String str) {
    if (str == null || str.isEmpty()) {
      return false;
    }
    return TIMESTAMP_PATTERN.matcher(str).matches();
  }

  /**
   * 对时间字符串进行标准化处理
   * 将秒级精度统一，忽略小的时间差异
   */
  private String normalizeTimeString(String timeStr) {
    try {
      SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
      long timeMillis = sdf.parse(timeStr).getTime();

      // 获取系统配置的时间精度容忍度
      long toleranceMs = getTimeToleranceMs();

      // 将时间向下取整到容忍度的倍数
      // 例如：如果容忍度是2000ms，时间戳14:02:52.123会被标准化为14:02:52.000
      // 14:02:49.456也会被标准化为14:02:48.000，这样在容忍范围内的时间会被视为相同
      long normalizedMillis = (timeMillis / toleranceMs) * toleranceMs;

      String result = sdf.format(new java.util.Date(normalizedMillis));
      return result;
    } catch (ParseException e) {
      LOGGER.debug("Failed to parse time string: {}, error: {}", timeStr, e.getMessage());
      return timeStr;
    }
  }

  /**
   * 对时间戳进行标准化处理
   * 支持10位秒级时间戳和13位毫秒级时间戳
   */
  private String normalizeTimestamp(String timestampStr) {
    try {
      long timestamp = Long.parseLong(timestampStr);

      // 如果是10位时间戳（秒级），转换为毫秒级
      if (timestampStr.length() == 10) {
        timestamp = timestamp * 1000;
      }

      // 获取系统配置的时间精度容忍度
      long toleranceMs = getTimeToleranceMs();

      // 将时间向下取整到容忍度的倍数
      long normalizedMillis = (timestamp / toleranceMs) * toleranceMs;

      // 返回标准化后的时间戳字符串（保持原始格式）
      if (timestampStr.length() == 10) {
        return String.valueOf(normalizedMillis / 1000);
      } else {
        return String.valueOf(normalizedMillis);
      }
    } catch (NumberFormatException e) {
      LOGGER.debug("Failed to parse timestamp: {}, error: {}", timestampStr, e.getMessage());
      return timestampStr;
    }
  }

  /**
   * 获取时间容忍度配置
   */
  private long getTimeToleranceMs() {
    try {
      SystemConfigWithProperties config = compareConfigService.getComparisonSystemConfig();
      long tolerance = config.getCompareIgnoreTimePrecisionMillis();
      return tolerance;
    } catch (Exception e) {
      LOGGER.info("Failed to get time tolerance from config, using default: {} ms", DEFAULT_TIME_TOLERANCE_MS);
      return DEFAULT_TIME_TOLERANCE_MS;
    }
  }

  /**
   * 检查字符串是否为有效的JSON格式
   * 
   * @param str 待检查的字符串
   * @return true如果是有效JSON，false否则
   */
  private boolean isValidJsonString(String str) {
    if (str == null || str.trim().isEmpty()) {
      return false;
    }
    
    str = str.trim();
    
    // 检查是否以JSON对象或数组的格式开始和结束（支持转义字符）
    if ((str.startsWith("{") && str.endsWith("}")) || 
        (str.startsWith("[") && str.endsWith("]"))) {
      try {
        // 尝试直接解析
        com.alibaba.fastjson2.JSON.parse(str);
        return true;
      } catch (Exception e) {
        // 如果直接解析失败，可能包含转义字符
        // 尝试将字符串作为JSON字符串值解析来处理转义
        try {
          // 构造一个包含该字符串的JSON对象，然后提取字符串值
          String jsonWrapper = "{\"value\":\"" + str.replace("\"", "\\\"") + "\"}";
          com.alibaba.fastjson2.JSONObject wrapper = com.alibaba.fastjson2.JSON.parseObject(jsonWrapper);
          String unescaped = wrapper.getString("value");
          com.alibaba.fastjson2.JSON.parse(unescaped);
          return true;
        } catch (Exception e2) {
          // 最后尝试：直接替换常见的转义字符
          try {
            String processed = str.replace("\\\"", "\"").replace("\\\\", "\\");
            com.alibaba.fastjson2.JSON.parse(processed);
            return true;
          } catch (Exception e3) {
            return false;
          }
        }
      }
    }
    
    return false;
  }

  private void mergeResult(String operation, String category, ReplayCompareResult diffResult,
      CompareResult sdkResult, CompareItem source, CompareItem target) {
    diffResult.setOperationName(operation);
    diffResult.setCategoryName(category);
    diffResult.setBaseMsg(sdkResult.getProcessedBaseMsg());
    diffResult.setTestMsg(sdkResult.getProcessedTestMsg());
    diffResult.setLogs(sdkResult.getLogs());
    diffResult.setMsgInfo(sdkResult.getMsgInfo());
    diffResult.setDiffResultCode(sdkResult.getCode());
    diffResult.setRecordTime(source != null ? source.getCreateTime() : MAX_TIME);
    diffResult.setReplayTime(target != null ? target.getCreateTime() : MAX_TIME);
    diffResult.setInstanceId(target != null ? target.getCompareKey() : Objects.requireNonNull(
        source).getCompareKey());
    diffResult.setServiceName(diffResult.getServiceName());
  }

  private boolean ignoreCategory(String operationType, String operationName,
      List<CategoryDetail> ignoreCategoryTypes) {
    if (CollectionUtils.isEmpty(ignoreCategoryTypes)) {
      return false;
    }
    for (CategoryDetail categoryDetail : ignoreCategoryTypes) {
      if (Objects.equals(categoryDetail.getOperationType(), operationType) && (
          categoryDetail.getOperationName() == null ||
              Objects.equals(categoryDetail.getOperationName(), operationName))) {
        return true;
      }
    }
    return false;
  }
}