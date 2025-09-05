package com.vision.wb;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.vision.dao.AbstractDao;
import com.vision.dao.LevelOfDisplayDao;
import com.vision.exception.RuntimeCustomException;
import com.vision.util.ValidationUtil;
import com.vision.vb.CommonVb;
import com.vision.vb.LevelOfDisplayUserVb;
import com.vision.vb.LevelOfDisplayVb;
import com.vision.vb.ProfileData;

@Component
public class LevelOfDisplayWb<E extends CommonVb> extends AbstractWorkerBean {

	@Autowired
	LevelOfDisplayDao levelOfDisplayDao;

	public List<LevelOfDisplayVb> getQueryUserGroupProfile() {
		List<LevelOfDisplayVb> treeStructuredProfile = new ArrayList<LevelOfDisplayVb>();
		String conditionalUserGrp = "";
		try {
			List<LevelOfDisplayVb> lodVbList = levelOfDisplayDao.getQueryUserGroupProfile();
			for (LevelOfDisplayVb lodVb : lodVbList) {
				if (!conditionalUserGrp.equalsIgnoreCase(lodVb.getUserGroup())) {
					String tempUserGrp = lodVb.getUserGroup();
					List<LevelOfDisplayVb> filteredProfile = (List<LevelOfDisplayVb>) lodVbList.stream()
							.filter(s -> s.getUserGroup().equalsIgnoreCase(tempUserGrp)).collect(Collectors.toList());
					conditionalUserGrp = tempUserGrp;
					treeStructuredProfile.add(new LevelOfDisplayVb(conditionalUserGrp, "", filteredProfile));
				}
			}
			return treeStructuredProfile;
		} catch (Exception e) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				e.printStackTrace();
			}
			throw new RuntimeCustomException("Failed to retrieve user group and profile.");
		}
	}

	public List<Object> getQueryUserGroup() {
		List<Object> collTemp = new ArrayList<Object>();
		try {
			List<ProfileData> profileDataList = levelOfDisplayDao.getQueryUserGroup();
			if (profileDataList != null && !profileDataList.isEmpty()) {
				for (ProfileData profileData : profileDataList) {
					collTemp.add(profileData.getUserGroup());
				}
			}
			return collTemp;
		} catch (Exception e) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				e.printStackTrace();
			}
			throw new RuntimeCustomException("Failed to retrieve user group.");
		}
	}

	public List<Object> getUserListExcludingUserGrpProf(LevelOfDisplayUserVb levelOfDisplayUserVb) {
		try {
			if (ValidationUtil.isValid(levelOfDisplayUserVb.getUserGroup())
					&& ValidationUtil.isValid(levelOfDisplayUserVb.getUserProfile())) {
				StringBuffer usrGroupAndProfile = new StringBuffer();
				usrGroupAndProfile
						.append(levelOfDisplayUserVb.getUserGroup() + "-" + levelOfDisplayUserVb.getUserProfile());
				return levelOfDisplayDao.getUserListExcludingUserGrpProf(String.valueOf(usrGroupAndProfile));
			} else {
				throw new RuntimeCustomException("User group or User Profile is not valid.");
			}
		} catch (Exception e) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				e.printStackTrace();
			}
			throw new RuntimeCustomException("Failed to retrieve user group based profile.");
		}
	}

	public List<Object> getQueryUserGroupBasedProfile(String userGroup) {
		List<Object> collTemp = new ArrayList<Object>();
		try {
			List<ProfileData> profileDataList = levelOfDisplayDao.getQueryUserGroupBasedProfile(userGroup);
			if (profileDataList != null && !profileDataList.isEmpty()) {
				for (ProfileData profileData : profileDataList) {
					collTemp.add(profileData.getUserProfile());
				}
			}
			return collTemp;
		} catch (Exception e) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				e.printStackTrace();
			}
			throw new RuntimeCustomException("Failed to retrieve user group based profile.");
		}
	}

	@Override
	protected AbstractDao getScreenDao() {
		return null;
	}

	@Override
	protected void setAtNtValues(CommonVb vObject) {
	}

	@Override
	protected void setVerifReqDeleteType(CommonVb vObject) {
	}

}
