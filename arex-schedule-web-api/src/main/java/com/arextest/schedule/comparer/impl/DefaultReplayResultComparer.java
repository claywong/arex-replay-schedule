package com.arextest.schedule.comparer.impl;

import com.alibaba.fastjson2.JSONObject;
import com.arextest.diff.model.CompareOptions;
import com.arextest.diff.model.CompareResult;
import com.arextest.diff.model.enumeration.DiffResultCode;
import com.arextest.diff.sdk.CompareSDK;
import com.arextest.model.mock.MockCategoryType;
import com.arextest.schedule.comparer.CategoryComparisonHolder;
import com.arextest.schedule.comparer.CompareConfigService;
import com.arextest.schedule.comparer.CompareItem;
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
import com.arextest.web.model.contract.contracts.compare.CategoryDetail;
import com.arextest.web.model.contract.contracts.config.SystemConfigWithProperties;
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

@Slf4j
@Builder
public class DefaultReplayResultComparer implements ReplayResultComparer {

  private static final CompareSDK COMPARE_INSTANCE = new CompareSDK();
  private static final long MAX_TIME = Long.MAX_VALUE;

  private final CompareConfigService compareConfigService;
  private final PrepareCompareSourceRemoteLoader sourceRemoteLoader;
  private final ProgressTracer progressTracer;
  private final ComparisonWriter comparisonOutputWriter;
  private final ReplayActionCaseItemRepository caseItemRepository;
  private final MetricService metricService;
  private final CustomComparisonConfigurationHandler configHandler;


  public DefaultReplayResultComparer(CompareConfigService compareConfigService,
      PrepareCompareSourceRemoteLoader sourceRemoteLoader,
      ProgressTracer progressTracer,
      ComparisonWriter comparisonOutputWriter,
      ReplayActionCaseItemRepository caseItemRepository,
      MetricService metricService,
      CustomComparisonConfigurationHandler configHandler) {
    this.compareConfigService = compareConfigService;
    this.sourceRemoteLoader = sourceRemoteLoader;
    this.progressTracer = progressTracer;
    this.comparisonOutputWriter = comparisonOutputWriter;
    this.caseItemRepository = caseItemRepository;
    this.metricService = metricService;
    this.configHandler = configHandler;
    addGlobalOptionToSDK(compareConfigService);
  }

  public void addGlobalOptionToSDK(CompareConfigService compareConfigService) {
    SystemConfigWithProperties comparisonSystemConfig = compareConfigService.getComparisonSystemConfig();
    COMPARE_INSTANCE.getGlobalOptions()
        .putPluginJarUrl(comparisonSystemConfig.getComparePluginInfo() == null ? null
            : comparisonSystemConfig.getComparePluginInfo().getComparePluginUrl())
        .putNameToLower(comparisonSystemConfig.getCompareNameToLower())
        .putNullEqualsEmpty(comparisonSystemConfig.getCompareNullEqualsEmpty())
        .putIgnoredTimePrecision(comparisonSystemConfig.getCompareIgnoreTimePrecisionMillis())
        .putIgnoreNodeSet(comparisonSystemConfig.getIgnoreNodeSet())
        .putSelectIgnoreCompare(comparisonSystemConfig.getSelectIgnoreCompare())
        .putOnlyCompareCoincidentColumn(comparisonSystemConfig.getOnlyCompareCoincidentColumn())
        .putUuidIgnore(comparisonSystemConfig.getUuidIgnore())
        .putIpIgnore(comparisonSystemConfig.getIpIgnore());
  }

  public static CompareSDK getCompareSDKInstance() {
    return COMPARE_INSTANCE;
  }

  @Override
  public boolean compare(ReplayActionCaseItem caseItem, boolean useReplayId) {
    StopWatch compareWatch = new StopWatch();
    compareWatch.start(LogType.COMPARE.getValue());
    String planId = caseItem.getParent().getPlanId();
    try {
      MDCTracer.addPlanId(planId);
      MDCTracer.addPlanItemId(caseItem.getPlanItemId());

      List<CategoryComparisonHolder> waitCompareMap =
          sourceRemoteLoader.buildWaitCompareList(caseItem, useReplayId);
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
        } else if (replayCompareResult.getDiffResultCode()
            == DiffResultCode.COMPARED_INTERNAL_EXCEPTION) {
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
   * compare recording and replay data. 1. record and replay data through compareKey.
   */
  private List<ReplayCompareResult> compareReplayResult(CategoryComparisonHolder bindHolder,
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

    Map<String, List<CompareItem>> recordMap =
        recordResults.stream().filter(data -> StringUtils.isNotEmpty(data.getCompareKey()))
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
        caseItem.getCompareMode().getValue());
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
      ReplayComparisonConfig compareConfig, int compareMode) {
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

      if (compareMode == CompareModeType.FULL.getValue()) {
        return COMPARE_INSTANCE.compare(decodedRecord, decodedResult, options);
      }
      return COMPARE_INSTANCE.quickCompare(decodedRecord, decodedResult, options);

    } catch (Throwable e) {
      LOGGER.error("run compare sdk process error:{} ,source: {} ,target:{}", e.getMessage(),
          record, result);
      return CompareSDK.fromException(record, result, e.getMessage());
    }
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