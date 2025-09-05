package com.vision.wb;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.vision.authentication.CustomContextHolder;
import com.vision.dao.CommonDao;
import com.vision.dao.VisionUsersDao;
import com.vision.exception.RuntimeCustomException;
import com.vision.util.ValidationUtil;
import com.vision.vb.MenuVb;
import com.vision.vb.ProfileData;
import com.vision.vb.UserRestrictionVb;
import com.vision.vb.VisionUsersVb;

@Service
public class LoginServicesWb {

	public static Logger logger = LoggerFactory.getLogger(LoginServicesWb.class);

	@Autowired
	private CommonDao commonDao;
	
	@Autowired
	private VisionUsersDao visionUsersDao;

	public LinkedHashMap<String, Object> getMenuForUser() {
		LinkedHashMap<String, Object> responseMap = new LinkedHashMap<String, Object>();
		VisionUsersVb lCurrentUser = CustomContextHolder.getContext();
		ArrayList<MenuVb> resultMenu = new ArrayList<MenuVb>();
		try {
			// List<ProfileData> topLvlMenuList = commonDao.getTopLevelMenu(lCurrentUser.getVisionId());
			List<ProfileData> topLvlGroupList = commonDao.getTopLevelMenu();
			List<ProfileData> finalMenuGrouplst = new ArrayList<ProfileData>();
			if (topLvlGroupList != null && !topLvlGroupList.isEmpty()) {
				for (ProfileData topLvlGroupVb : topLvlGroupList) {
					ArrayList<MenuVb> resultChilds = new ArrayList<MenuVb>();
					MenuVb lMenuVb = new MenuVb();
					lMenuVb.setMenuName(topLvlGroupVb.getMenuItem());
					lMenuVb.setMenuGroup(topLvlGroupVb.getMenuGroup());
					lMenuVb.setMenuIcon(topLvlGroupVb.getMenuIcon());
					lMenuVb.setMenuProgram(topLvlGroupVb.getMenuProgram());
					lMenuVb.setRecordIndicator(0);
					lMenuVb.setMenuStatus(0);
					ArrayList<MenuVb> subMenuList = commonDao.getSubMenuItemsForMenuGroup(topLvlGroupVb.getMenuGroup());
					for (MenuVb menuVb : subMenuList) {
						ArrayList<MenuVb> screenPrivilegesList = commonDao.getSubMenuItemsForSubMenuGroup(
								topLvlGroupVb.getMenuGroup(), menuVb.getParentSequence(), lCurrentUser.getVisionId());
						if (screenPrivilegesList != null && screenPrivilegesList.size() > 0) {
							if ("PilotConsole".equalsIgnoreCase(menuVb.getMenuProgram())) {
								responseMap.put("PilotConsole", new ArrayList<MenuVb>(Arrays.asList(screenPrivilegesList.get(0))));
							} else {
								menuVb.setChildren(screenPrivilegesList);
								resultChilds.add(menuVb);
							}
						} else if(screenPrivilegesList.size() == 0 && "PilotConsole".equalsIgnoreCase(menuVb.getMenuProgram())) {
							responseMap.put("PilotConsole", new ArrayList<>());
						}
					}
					
					if ((resultChilds != null && resultChilds.size() > 0)) {
						if (!"PilotConsole".equalsIgnoreCase(topLvlGroupVb.getMenuProgram())) {
							lMenuVb.setChildren(resultChilds);
							finalMenuGrouplst.add(topLvlGroupVb);
							resultMenu.add(lMenuVb);
						}
					}
				}
			}
			responseMap.put("menu_details", finalMenuGrouplst);
			responseMap.put("menu_hierarchy", resultMenu);
		} catch (Exception e) {
		//	logger.error("Exception in getting menu for the user[" + lCurrentUser.getVisionId() + "]. : " + e.getMessage(), e);
			throw new RuntimeCustomException("Failed to retrieve menu for your profile. Please contact System Admin.");
		}
		return responseMap;
	}
	
	public VisionUsersVb getUserDetails() {
		try {
			VisionUsersVb userVb = CustomContextHolder.getContext();
			/* If product level access log is needed, write logic below */
//			visionUsersDao.updateActivityDateByUserLoginId(lUser);
			if ("Y".equalsIgnoreCase(userVb.getUpdateRestriction())) {
				/* Update restriction - Start */
				List<UserRestrictionVb> restrictionList = visionUsersDao.getRestrictionTree();
				Iterator<UserRestrictionVb> restrictionItr = restrictionList.iterator();
				while(restrictionItr.hasNext()) {
					UserRestrictionVb restrictionVb = restrictionItr.next();
					restrictionVb.setRestrictionSql(visionUsersDao.getVisionDynamicHashVariable(restrictionVb.getMacrovarName()));
				}
				restrictionList = visionUsersDao.doUpdateRestrictionToUserObject(userVb, restrictionList);
				userVb.setRestrictionList(restrictionList);
				/* Update restriction - End */
			}
			String homeDashboard = commonDao.getUserHomeDashboard(userVb.getUserGroup()+"-"+userVb.getUserProfile());
			if(!ValidationUtil.isValid(homeDashboard))
				homeDashboard = "NA";
			userVb.setHomeDashboard(homeDashboard);
			return userVb;
		} catch (Exception e) {
			throw new RuntimeCustomException("Problem in geting user detail. Please contact System Admin.");
		}
	}

}
