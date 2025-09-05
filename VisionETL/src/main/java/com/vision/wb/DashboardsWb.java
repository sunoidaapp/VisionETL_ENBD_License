package com.vision.wb;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.vision.dao.AbstractDao;
import com.vision.dao.AlphaSubTabDao;
import com.vision.dao.DashboardsDao;
import com.vision.exception.ExceptionCode;
import com.vision.exception.RuntimeCustomException;
import com.vision.util.CommonUtils;
import com.vision.util.Constants;
import com.vision.util.ValidationUtil;
import com.vision.vb.AlphaSubTabVb;
import com.vision.vb.DashboardFilterVb;
import com.vision.vb.DashboardTabVb;
import com.vision.vb.DashboardTilesVb;
import com.vision.vb.DashboardVb;
	
@Component
public class DashboardsWb extends AbstractWorkerBean<DashboardVb>{
	@Autowired
	private DashboardsDao  dashboardsDao;
	@Autowired
	private AlphaSubTabDao alphaSubTabDao;
	
	//Component 1 - Dashboard Detail and Tab Detail
		public ExceptionCode getDashboardDetail(DashboardVb dObj){
			ExceptionCode exceptionCode = new ExceptionCode();
			try{ 
	 			DashboardVb mdmDashboardVb = dashboardsDao.getDashboardDetail(dObj);
	 			List<DashboardTabVb> tabslst = dashboardsDao.getDashboardTabDetails(dObj);
	 			if("Y".equalsIgnoreCase(mdmDashboardVb.getFilterFlag())) {
	 				List<DashboardFilterVb> dashboardFilterlst = dashboardFilterProcess(mdmDashboardVb.getFilterRefCode());
	 				mdmDashboardVb.setDashboardFilterlst(dashboardFilterlst);
	 			}
	 			ArrayList<DashboardTabVb> tabDetails =  new ArrayList<DashboardTabVb>();
	 			ArrayList<DashboardTilesVb> tileslstAll = new ArrayList<DashboardTilesVb>();
	 			tabslst.forEach(tabsVb -> {
	 				tabsVb.setDashboardId(dObj.getDashboardId());
	 				List<DashboardTilesVb> Tileslst = dashboardsDao.getDashboardTileDetails(tabsVb);
	 				tabsVb.setTileDetails(Tileslst);
	 				tabDetails.add(tabsVb);
	 			});
	 			mdmDashboardVb.setDashboardTabs(tabDetails);
	 			exceptionCode.setResponse(mdmDashboardVb);
	 			exceptionCode.setErrorCode(Constants.SUCCESSFUL_OPERATION);
				return exceptionCode;
			}catch(Exception ex){
				exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
				exceptionCode.setErrorMsg(ex.getMessage());
				return exceptionCode;
			}
		}
	//Component 2 - Get the Tab result by passing Tab Id and Dashboard Id
	/*public ExceptionCode getTabResultData(MdmDashboardTabVb dObj){
		ExceptionCode exceptionCode = new ExceptionCode();
		try{ 
			promptCheck(dObj);
 			List<MdmDashboardTilesVb> resultlst = getTileResults(dObj);
 			exceptionCode.setResponse(resultlst);
 			exceptionCode.setOtherInfo(dObj);
 			exceptionCode.setErrorCode(Constants.SUCCESSFUL_OPERATION);
			return exceptionCode;
		}catch(Exception ex){
			exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
			exceptionCode.setErrorMsg(ex.getMessage());
			return exceptionCode;
		}
	}*/
	/*public List<MdmDashboardTilesVb> getTileResults(MdmDashboardTabVb dObj) throws SQLException{
		List<MdmDashboardTilesVb> tabTileslst = dashboardsDao.getDashboardTileDetails(dObj);
		ArrayList<MdmDashboardTilesVb> resultlst = new ArrayList<>();
		if(tabTileslst != null && tabTileslst.size() > 0) {
			List<DataFetcher> threads = new ArrayList<DataFetcher>(tabTileslst.size());
			tabTileslst.forEach(dashboardTilesVb -> {
				DataFetcher fetcher = new DataFetcher(dashboardTilesVb,dObj);
				fetcher.setDaemon(true);
				fetcher.start();
				try {
					fetcher.join();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					// e.printStackTrace();
				}
				threads.add(fetcher);
			});
			for(DataFetcher df:threads){
				while(!df.dataFetched){
					try {
						Thread.sleep(150);
					} catch (InterruptedException e) {
						// e.printStackTrace();
					}
				}
				resultlst.add(df.dashboardTilesVb);
			}
			
			for(DataFetcher df:threads){
				int count = 0;
				if(!df.dataFetched){
					try {
						Thread.sleep(2000);
					} catch (InterruptedException e) {
						// e.printStackTrace();
					}
					count++;
					if(count > 150){
						count = 0;
						logger.info("Data fetch in progress for the report :"+ df.toString());
						continue;
					}
				}
				resultlst.add(df.dashboardTilesVb);
			}
			for(MdmDashboardTilesVb dashboardTilesVb : tabTileslst) {
				resultlst.add(dashboardTilesVb);
			}
		}
		return resultlst;
	}*/
	@Override
	protected void setAtNtValues(DashboardVb vObject) {
	}
	@Override
	protected void setVerifReqDeleteType(DashboardVb vObject) {
		vObject.setStaticDelete(false);
		vObject.setVerificationRequired(false);
	}
	@Override
	protected AbstractDao<DashboardVb> getScreenDao() {
		return dashboardsDao;
	}
	
	class DataFetcher extends Thread {
		boolean dataFetched = false;
		boolean errorOccured = false;
		String errorMsg = "";
		ExceptionCode exceptionCode;
		DashboardTilesVb dashboardTilesVb;
		DashboardTabVb dObj;

		public DataFetcher(DashboardTilesVb dashboardTilesVb) {
			this.dashboardTilesVb = dashboardTilesVb;
		}

		public void run() {
			try {
				if ("T".equalsIgnoreCase(dashboardTilesVb.getTileType())) {
					String tileResult = dashboardsDao.getTilesReportData(dashboardTilesVb);
					dashboardTilesVb.setTileDataSet(tileResult);
					List<DashboardTilesVb> drillDowntabTileslst = dashboardsDao.getTileDrillDownDetails(
							dashboardTilesVb.getDashboardId(), dashboardTilesVb.getTabId(),
							dashboardTilesVb.getTileSequence(), dashboardTilesVb.getSubSequence(), true);
					dashboardTilesVb.setDrillDownlst(drillDowntabTileslst);
					if ("Y".equalsIgnoreCase(dashboardTilesVb.getDoubleWidthFlag())) {
						dashboardTilesVb.setColumns("col-sm-12 col-lg-4 col-xl-4 col-md-4");
					} else {
						dashboardTilesVb.setColumns("col-sm-12 col-lg-2 col-xl-2 col-md-2");
					}
				} else if ("C".equalsIgnoreCase(dashboardTilesVb.getTileType())) {
					Connection con = null;
					String chartResult = dashboardsDao.getChartReportData(dashboardTilesVb, con);
					dashboardTilesVb.setChartDataSet(chartResult);
					List<DashboardTilesVb> drillDowntabTileslst = dashboardsDao.getTileDrillDownDetails(
							dashboardTilesVb.getDashboardId(), dashboardTilesVb.getTabId(),
							dashboardTilesVb.getTileSequence(), dashboardTilesVb.getSubSequence(), true);
					dashboardTilesVb.setDrillDownlst(drillDowntabTileslst);
					if ("Y".equalsIgnoreCase(dashboardTilesVb.getDoubleWidthFlag())) {
						dashboardTilesVb.setColumns("col-sm-12 col-lg-4 col-xl-4 col-md-4");
					} else {
						dashboardTilesVb.setColumns("col-sm-12 col-lg-2 col-xl-2 col-md-2");
					}
					if (con != null) {
						con.close();
					}
				} else if ("G".equalsIgnoreCase(dashboardTilesVb.getTileType())) {
					List gridDatalst = dashboardsDao.getGridData(dashboardTilesVb);
					dashboardTilesVb.setGridColumnSet((List) gridDatalst.get(0));
					dashboardTilesVb.setGridDataSet((List) gridDatalst.get(1));
					dashboardTilesVb.setGridColumnFormats((List) gridDatalst.get(2));
					List<DashboardTilesVb> drillDowntabTileslst = dashboardsDao.getTileDrillDownDetails(
							dashboardTilesVb.getDashboardId(), dashboardTilesVb.getTabId(),
							dashboardTilesVb.getTileSequence(), dashboardTilesVb.getSubSequence(), true);
					dashboardTilesVb.setDrillDownlst(drillDowntabTileslst);
					if ("Y".equalsIgnoreCase(dashboardTilesVb.getDoubleWidthFlag())) {
						dashboardTilesVb.setColumns("col-sm-12 col-lg-4 col-xl-4 col-md-4");
					} else {
						dashboardTilesVb.setColumns("col-sm-12 col-lg-2 col-xl-2 col-md-2");
					}
				}
				dataFetched = true;
				errorOccured = false;
			} catch (RuntimeCustomException rex) {
				dataFetched = true;
				errorOccured = true;
				exceptionCode = rex.getCode();
			} catch (Exception e) {
				dataFetched = true;
				errorOccured = true;
				errorMsg = e.getMessage();
			}
		}
	}

	public ExceptionCode getTileResultData(DashboardTilesVb vObject) {
		ExceptionCode exceptionCode = new ExceptionCode();
		try {
			Connection con = null;
			DashboardTilesVb childTileObj = new DashboardTilesVb();
			ArrayList<DashboardTilesVb> childTilelst = new ArrayList<DashboardTilesVb>();
			if ("T".equalsIgnoreCase(vObject.getTileType())) {
				if ("Y".equalsIgnoreCase(vObject.getSubTiles())) {
					List<DashboardTilesVb> subTilelst = dashboardsDao.getDashboardSubTileDetails(vObject);
					ArrayList<DashboardTilesVb> resultlst = new ArrayList<>();
					if (subTilelst != null && subTilelst.size() > 0) {
						List<DataFetcher> threads = new ArrayList<DataFetcher>(subTilelst.size());
						subTilelst.forEach(subTileVb -> {
							subTileVb.setPromptValue1(vObject.getPromptValue1());
							subTileVb.setPromptValue2(vObject.getPromptValue2());
							subTileVb.setPromptValue3(vObject.getPromptValue3());
							subTileVb.setPromptValue4(vObject.getPromptValue4());
							subTileVb.setPromptValue5(vObject.getPromptValue5());
							subTileVb.setPromptValue6(vObject.getPromptValue6());
							subTileVb.setPromptValue7(vObject.getPromptValue7());
							subTileVb.setPromptValue8(vObject.getPromptValue8());
							subTileVb.setPromptValue9(vObject.getPromptValue9());
							subTileVb.setPromptValue10(vObject.getPromptValue10());
							DataFetcher fetcher = new DataFetcher(subTileVb);
							fetcher.setDaemon(true);
							fetcher.start();
							try {
								fetcher.join();
							} catch (InterruptedException e) {
								if ("Y".equalsIgnoreCase(enableDebug)) {
									e.printStackTrace();
								}
							}
							threads.add(fetcher);
						});
						for (DataFetcher df : threads) {
							int count = 0;
							if (!df.dataFetched) {
								try {
									Thread.sleep(2000);
								} catch (InterruptedException e) {
									if ("Y".equalsIgnoreCase(enableDebug)) {
										e.printStackTrace();
									}
								}
								count++;
								if (count > 150) {
									count = 0;
									logger.info("Data fetch in progress for the report :" + df.toString());
									continue;
								}
							}
						}
						for (DashboardTilesVb dashboardTilesVb : subTilelst) {
							childTilelst.add(dashboardTilesVb);
						}
					}
					vObject.setChildTileslst(childTilelst);
				} else {
					String tileResult = dashboardsDao.getTilesReportData(vObject);
					if (!ValidationUtil.isValid(tileResult)) {
						exceptionCode.setErrorCode(Constants.NO_RECORDS_FOUND);
						exceptionCode.setErrorMsg("No Date Found!");
					}
					childTileObj.setTileDataSet(tileResult);
					List<DashboardTilesVb> drillDowntabTileslst = dashboardsDao.getTileDrillDownDetails(
							vObject.getDashboardId(), vObject.getTabId(), vObject.getTileSequence(),
							vObject.getSubSequence(), false);
					childTileObj.setDrillDownlst(drillDowntabTileslst);
					childTilelst.add(childTileObj);
					vObject.setChildTileslst(childTilelst);
					if ("Y".equalsIgnoreCase(vObject.getDoubleWidthFlag())) {
						vObject.setColumns("col-sm-12 col-lg-4 col-xl-4 col-md-4");
					} else {
						vObject.setColumns("col-sm-12 col-lg-2 col-xl-2 col-md-2");
					}
				}
			} else if ("C".equalsIgnoreCase(vObject.getTileType())) {
				if ("Y".equalsIgnoreCase(vObject.getSubTiles())) {
					List<DashboardTilesVb> subTilelst = dashboardsDao.getDashboardSubTileDetails(vObject);
					ArrayList<DashboardTilesVb> resultlst = new ArrayList<>();
					if (subTilelst != null && subTilelst.size() > 0) {
						List<DataFetcher> threads = new ArrayList<DataFetcher>(subTilelst.size());
						subTilelst.forEach(subTileVb -> {
							subTileVb.setPromptValue1(vObject.getPromptValue1());
							subTileVb.setPromptValue2(vObject.getPromptValue2());
							subTileVb.setPromptValue3(vObject.getPromptValue3());
							subTileVb.setPromptValue4(vObject.getPromptValue4());
							subTileVb.setPromptValue5(vObject.getPromptValue5());
							subTileVb.setPromptValue6(vObject.getPromptValue6());
							subTileVb.setPromptValue7(vObject.getPromptValue7());
							subTileVb.setPromptValue8(vObject.getPromptValue8());
							subTileVb.setPromptValue9(vObject.getPromptValue9());
							subTileVb.setPromptValue10(vObject.getPromptValue10());
							DataFetcher fetcher = new DataFetcher(subTileVb);
							fetcher.setDaemon(true);
							fetcher.start();
							try {
								fetcher.join();
							} catch (InterruptedException e) {
								if ("Y".equalsIgnoreCase(enableDebug)) {
									e.printStackTrace();
								}
							}
							threads.add(fetcher);
						});
						for (DataFetcher df : threads) {
							int count = 0;
							if (!df.dataFetched) {
								try {
									Thread.sleep(2000);
								} catch (InterruptedException e) {
									if ("Y".equalsIgnoreCase(enableDebug)) {
										e.printStackTrace();
									}
								}
								count++;
								if (count > 150) {
									count = 0;
									logger.info("Data fetch in progress for the report :" + df.toString());
									continue;
								}
							}
						}
						for (DashboardTilesVb dashboardTilesVb : subTilelst) {
							childTilelst.add(dashboardTilesVb);
						}
					}
					vObject.setChildTileslst(childTilelst);
				} else {
					String chartResult = dashboardsDao.getChartReportData(vObject, con);
					if (!ValidationUtil.isValid(chartResult)) {
						exceptionCode.setErrorCode(Constants.NO_RECORDS_FOUND);
						exceptionCode.setErrorMsg("No Date Found!");
					}
					childTileObj.setChartDataSet(chartResult);
					List<DashboardTilesVb> drillDowntabTileslst = dashboardsDao.getTileDrillDownDetails(
							vObject.getDashboardId(), vObject.getTabId(), vObject.getTileSequence(),
							vObject.getSubSequence(), false);
					childTileObj.setDrillDownlst(drillDowntabTileslst);
					childTilelst.add(childTileObj);
					vObject.setChildTileslst(childTilelst);
					if ("Y".equalsIgnoreCase(vObject.getDoubleWidthFlag())) {
						vObject.setColumns("col-sm-12 col-lg-6 col-xl-6 col-md-6");
					} else {
						vObject.setColumns("col-sm-12 col-lg-4 col-xl-4 col-md-4");
					}
				}
				if (con != null) {
					con.close();
				}
			} else if ("G".equalsIgnoreCase(vObject.getTileType())) {
				List gridDatalst = dashboardsDao.getGridData(vObject);
				childTileObj.setGridColumnSet((List) gridDatalst.get(0));
				if (!ValidationUtil.isValid(childTileObj.getGridColumnSet())) {
					exceptionCode.setErrorCode(Constants.NO_RECORDS_FOUND);
					exceptionCode.setErrorMsg("Columns not configured!");
				}
				childTileObj.setGridDataSet((List) gridDatalst.get(1));
				if (!ValidationUtil.isValid(childTileObj.getGridDataSet())) {
					exceptionCode.setErrorCode(Constants.NO_RECORDS_FOUND);
					exceptionCode.setErrorMsg("No Data Found!");
				}
				childTileObj.setGridColumnFormats((List) gridDatalst.get(2));
				if (!ValidationUtil.isValid(childTileObj.getGridColumnFormats())) {
					exceptionCode.setErrorCode(Constants.NO_RECORDS_FOUND);
					exceptionCode.setErrorMsg("Column Format not Configured!");
				}
				List<DashboardTilesVb> drillDowntabTileslst = dashboardsDao.getTileDrillDownDetails(
						vObject.getDashboardId(), vObject.getTabId(), vObject.getTileSequence(),
						vObject.getSubSequence(), false);
				childTileObj.setDrillDownlst(drillDowntabTileslst);
				childTilelst.add(childTileObj);
				vObject.setChildTileslst(childTilelst);
				if ("Y".equalsIgnoreCase(vObject.getDoubleWidthFlag())) {
					vObject.setColumns("col-sm-12 col-lg-6 col-xl-6 col-md-6");
				} else {
					vObject.setColumns("col-sm-12 col-lg-4 col-xl-4 col-md-4");
				}
			}
			exceptionCode.setResponse(vObject);
			exceptionCode.setOtherInfo(vObject);
			if (!ValidationUtil.isValid(exceptionCode.getErrorCode())) {
				exceptionCode.setErrorCode(Constants.SUCCESSFUL_OPERATION);
			}
			return exceptionCode;
		} catch (Exception ex) {
			exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
			exceptionCode.setErrorMsg(ex.getMessage());
			return exceptionCode;
		}
	}

	public ExceptionCode getDrillDownData(List<DashboardTilesVb> drillDownlst) {
		ExceptionCode exceptionCode = new ExceptionCode();
		DashboardTilesVb dashboardTilesVb = new DashboardTilesVb();
		try {
			// List<MdmDashboardTilesVb> drillDowntabTileslst =
			// dashboardsDao.getTileDrillDownDetails(dObj.getDashboardId(),dObj.getTabId(),dObj.getTileSequence());
			List<DashboardTilesVb> drillDownResultlst = new ArrayList();
			if (drillDownlst != null && drillDownlst.size() > 0) {
				for (DashboardTilesVb dObj : drillDownlst) {
					if ("C".equalsIgnoreCase(dObj.getTileType())) {
						Connection con = null;
						String chartResult = dashboardsDao.getChartReportData(dObj, con);
						if (!ValidationUtil.isValid(chartResult)) {
							exceptionCode.setErrorCode(Constants.NO_RECORDS_FOUND);
							exceptionCode.setErrorMsg("No Date Found!");
						}
						dObj.setChartDataSet(chartResult);
						if ("Y".equalsIgnoreCase(dObj.getDoubleWidthFlag())) {
							dObj.setColumns("col-sm-12 col-lg-6 col-xl-6 col-md-6");
						} else {
							dObj.setColumns("col-sm-12 col-lg-4 col-xl-4 col-md-4");
						}
						if (con != null) {
							con.close();
						}
					} else if ("T".equalsIgnoreCase(dObj.getTileType())) {
						String drillDownTileResult = dashboardsDao.getTilesReportData(dObj);
						dObj.setTileDataSet(drillDownTileResult);
						if ("Y".equalsIgnoreCase(dObj.getDoubleWidthFlag())) {
							dObj.setColumns("col-sm-12 col-lg-4 col-xl-4 col-md-4");
						} else {
							dObj.setColumns("col-sm-12 col-lg-2 col-xl-2 col-md-2");
						}
					} else if ("G".equalsIgnoreCase(dObj.getTileType())) {
						List gridDatalst = dashboardsDao.getGridData(dObj);
						dObj.setGridColumnSet((List) gridDatalst.get(0));
						if (!ValidationUtil.isValid(dObj.getGridColumnSet())) {
							exceptionCode.setErrorCode(Constants.NO_RECORDS_FOUND);
							exceptionCode.setErrorMsg("Columns not configured!");
						}
						dObj.setGridDataSet((List) gridDatalst.get(1));
						if (!ValidationUtil.isValid(dObj.getGridDataSet())) {
							exceptionCode.setErrorCode(Constants.NO_RECORDS_FOUND);
							exceptionCode.setErrorMsg("No Data Found!");
						}
						dObj.setGridColumnFormats((List) gridDatalst.get(2));
						if (!ValidationUtil.isValid(dObj.getGridColumnFormats())) {
							exceptionCode.setErrorCode(Constants.NO_RECORDS_FOUND);
							exceptionCode.setErrorMsg("Column Format not Configured!");
						}
						if ("Y".equalsIgnoreCase(dObj.getDoubleWidthFlag())) {
							dObj.setColumns("col-sm-12 col-lg-6 col-xl-6 col-md-6");
						} else {
							dObj.setColumns("col-sm-12 col-lg-4 col-xl-4 col-md-4");
						}
					}
					drillDownResultlst.add(dObj);
				}
			}
			dashboardTilesVb.setDrillDownlst(drillDownResultlst);
			if (!ValidationUtil.isValid(exceptionCode.getErrorCode())) {
				exceptionCode.setErrorCode(Constants.SUCCESSFUL_OPERATION);
			}
			exceptionCode.setResponse(dashboardTilesVb);
			exceptionCode.setOtherInfo(drillDownlst);
		} catch (Exception e) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				e.printStackTrace();
			}
			exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
			exceptionCode.setErrorMsg("" + e.getMessage());
		}
		return exceptionCode;
	}
	
	//Filter Component
	public List<DashboardFilterVb> dashboardFilterProcess(String filterRefCode) {
		List<DashboardFilterVb> filterlst = new ArrayList<DashboardFilterVb>();
		try {
			List<DashboardFilterVb> filterDetaillst = dashboardsDao.getDashboardFilterDetail(filterRefCode);
			if (filterDetaillst != null && filterDetaillst.size() > 0) {
				DashboardFilterVb filterObj = filterDetaillst.get(0);
				for (int i = 1; i <= filterObj.getFilterCnt(); i++) {
					DashboardFilterVb vObject = new DashboardFilterVb();
					String filterObjProp = ValidationUtil.isValid(filterObj.getFilterRefXml())
							? filterObj.getFilterRefXml().replaceAll("\n", "").replaceAll("\r", "")
							: "";
					String refXml = CommonUtils.getValueForXmlTag(filterObjProp, "Prompt" + i);
					String filterLabel = CommonUtils.getValueForXmlTag(refXml, "Label");
					String filterType = CommonUtils.getValueForXmlTag(refXml, "Type");
					String filterSource = CommonUtils.getValueForXmlTag(refXml, "Source");
					String defaultValue = CommonUtils.getValueForXmlTag(refXml, "DefaultValue");
					String specificTab = CommonUtils.getValueForXmlTag(refXml, "SPECIFICTAB");
					String multiSelect = CommonUtils.getValueForXmlTag(refXml, "MULTISELECT");
					DashboardTilesVb tmpObj = new DashboardTilesVb();
					if (ValidationUtil.isValid(filterSource))
						filterSource = dashboardsDao.replacePromptVariables(filterSource, tmpObj);
					if (ValidationUtil.isValid(defaultValue))
						defaultValue = dashboardsDao.replacePromptVariables(defaultValue, tmpObj);

					if ("TEXT".equalsIgnoreCase(filterType) || "DATE".equalsIgnoreCase(filterType)
							|| "TEXTD".equalsIgnoreCase(filterType) || "TEXTM".equalsIgnoreCase(filterType)) {
						vObject.setFilterLabel(filterLabel);
						vObject.setFilterType(filterType);
						vObject.setFilterDefaultValue(dashboardsDao.getDashboardDefaultValue(defaultValue));
						vObject.setFilterSourceId(filterSource);
						vObject.setMultiSelect("N");
					}
					if ("COMBO".equalsIgnoreCase(filterType)) {
						vObject.setFilterLabel(filterLabel);
						vObject.setFilterType(filterType);
						vObject.setFilterSourceVal(dashboardsDao.getDashboardFilterValue(filterSource));
						vObject.setFilterDefaultValue(dashboardsDao.getDashboardDefaultValue(defaultValue));
						vObject.setMultiSelect(ValidationUtil.isValid(multiSelect) ? multiSelect : "N");
					}
					vObject.setSpecificTab(specificTab);
					filterlst.add(vObject);
				}
			}
			return filterlst;
		} catch (Exception e) {
			return null;
		}
	}
	public ExceptionCode getDashboardList() {
		ExceptionCode exceptionCode = new ExceptionCode();
		try {
			ArrayList<AlphaSubTabVb> dashboardGrplst = (ArrayList<AlphaSubTabVb>)alphaSubTabDao.findActiveAlphaSubTabsByAlphaTab(6016);
			ArrayList dashboardlst = new ArrayList();
			for(AlphaSubTabVb dashboardGrp : dashboardGrplst) {
				ArrayList dashboards = (ArrayList<AlphaSubTabVb>)dashboardsDao.getDashboadList(dashboardGrp.getAlphaSubTab());
				dashboardGrp.setChildren(dashboards);
				dashboardlst.add(dashboardGrp);
			}
			exceptionCode.setResponse(dashboardlst);
		} catch (Exception e) {
			exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
			exceptionCode.setErrorMsg(e.getMessage());
		}
		return exceptionCode;
	}
	
	@Override
	protected ExceptionCode doValidate(DashboardVb vObject){
		ExceptionCode exceptionCode = new ExceptionCode();
		String operation = vObject.getActionType();
		String srtRestrion = getCommonDao().getRestrictionsByUsers("", operation);
		if(!"Y".equalsIgnoreCase(srtRestrion)) {
			exceptionCode = new ExceptionCode();
			exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
			exceptionCode.setErrorMsg(operation +" "+Constants.userRestrictionMsg);
			return exceptionCode;
		}
		exceptionCode.setErrorCode(Constants.SUCCESSFUL_OPERATION);
		return exceptionCode;
	}
}

