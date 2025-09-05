package com.vision.wb;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.vision.dao.AbstractDao;
import com.vision.dao.MagnifierDao;
import com.vision.vb.MagnifierResultVb;

@Component
public class MagnifierWb extends AbstractWorkerBean<MagnifierResultVb>{
	@Autowired
	private MagnifierDao magnifierDao;
	
	@Override
	protected AbstractDao<MagnifierResultVb> getScreenDao() {
		return magnifierDao;
	}
	@Override 
	protected void setAtNtValues(MagnifierResultVb object) {
	}

	@Override
	protected void setVerifReqDeleteType(MagnifierResultVb object) {
	}
}