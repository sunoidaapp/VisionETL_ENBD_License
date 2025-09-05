package com.vision.util;

public class Constants {
	public static final int STATUS_ZERO, ERRONEOUS_OPERATION = STATUS_ZERO = 0;
	public static final int SUCCESSFUL_OPERATION,STATUS_UPDATE = SUCCESSFUL_OPERATION = 1;
	public static final int MODIFY_PENDING = 1;
	public static final int WE_HAVE_WARNING_DESCRIPTION = 36;
	public static final int INVALID_STATUS_FLAG_IN_DATABASE, STATUS_INSERT = INVALID_STATUS_FLAG_IN_DATABASE = 2;
	public static final int DELETE_PENDING = 3,DUPLICATE_KEY_INSERTION,STATUS_DELETE = DUPLICATE_KEY_INSERTION = 3;
	public static final int STATUS_PENDING, RECORD_PENDING_FOR_DELETION = STATUS_PENDING = 4;
	public static final int ATTEMPT_TO_MODIFY_UNEXISTING_RECORD = 5;
	public static final int ATTEMPT_TO_DELETE_UNEXISTING_RECORD = 6;
	public static final int TRYING_TO_DELETE_APPROVAL_PENDING_RECORD = 7;
	public static final int NO_SUCH_PENDING_RECORD = 8;
	public static final int NO_SUCH_PENDING_UPL_RECORD = 8;
	public static final int PENDING_FOR_ADD_ALREADY, PASSIVATE = PENDING_FOR_ADD_ALREADY = 9;
	public static final int RECORD_ALREADY_PRESENT_BUT_INACTIVE = 10;
	public static final int CANNOT_DELETE_AN_INACTIVE_RECORD = 11;
	public static final int NO_RECORDS_TO_APPROVE = 12;
	public static final int NO_RECORDS_TO_REJECT = 13;
	public static final int MAKER_CANNOT_APPROVE = 14;
	public static final int WE_HAVE_ERROR_DESCRIPTION = 20;
	public static final int PAGE_DISPLAY_SIZE, VALIDATION_NO_ERRORS = PAGE_DISPLAY_SIZE = 25;
	public static final int VALIDATION_ERRORS_FOUND = 26;
	public static final int AUDIT_TRAIL_ERROR = 30;
	public static final int BUILD_IS_RUNNING = 38;
	public static final int BUILD_IS_RUNNING_APPROVE = 39;
	public static final int FILE_UPLOAD_REMOTE_ERROR = 47;
	public static final int NO_RECORDS_FOUND = 41;
	public static final int FIN_ADJ_APPROVE_STATUS = 40;
	public static final int FIN_ADJ_INACTIVE_STATUS = 50;
	public static final int FIN_ADJ_DELETE_STATUS = 60;
	public static final int FINANCIAL_FETCH_SIZE = 500;
	public static final int YEAR_NOT_MAINTAIN = 49;
	public static final int LONG_MAX_LENGTH=19;
	public static final String ADD = "Add";
	public static final String MODIFY = "Modify";
	public static final String DELETE = "Delete";
	public static final String APPROVE = "Approve";
	public static final String REJECT = "Reject";
	public static final String COPY = "Copy";
	public static final String MOVE = "Move";
	public static final String QUERY = "Query";
	public static final String SAVE = "Save";
	public static final String Verification = "verification";
	public static final String Resubmission = "resubmission";
	public static final String Rejection = "rejection";
	public static final int DB_CONNECTION_ERROR = 50;
	public static final String PUBLISH = "Published";
	public static final String userRestrictionMsg = "Operation is restricted to your user profile.";

	public static final int PUBLISHED = 0;
	public static final int WORK_IN_PROGRESS = 1;
	public static final int PUBLISHED_AND_WORK_IN_PROGRESS = 2; 
	public static final int PUBLISHED_AND_DELETED = 9;

	public static final String EMPTY = "";	
	public static final String YES = "Y";
	public static final String NO = "N";
	public static final String UNKNOWN = "Unknown Cause. ";
	public static final String CONTACT_SYSTEM_ADMIN = "Contact System Admin.";
	public static final String TICK = "`";
	public static final String S_QUOTE = "'";
	public static final String D_QUOTE = "\"";
	public static final String COMMA = ",";
	public static final String UNDERSCORE = "_";
	
	
	/* DB Function conversion Strings - Start */
	public static final String DATEFUNC = "DATEFUNC";
	public static final String SYSDATE = "SYSDATE";
	public static final String NVL = "NVL";
	public static final String TIME = "TIME";
	public static final String DD_Mon_RRRR = "DD_Mon_RRRR";
	public static final String CONVERT = "CONVERT";
	public static final String TYPE = "TYPE";
	public static final String TIMEFORMAT = "TIMEFORMAT";
	public static final String PIPELINE = "PIPELINE";
	public static final String TO_DATE = "TO_DATE";
	public static final String LENGTH = "LENGTH";
	public static final String SUBSTR = "SUBSTR";
	public static final String TO_NUMBER = "TO_NUMBER";
	public static final String TO_CHAR = "TO_CHAR";
	public static final String DUAL = "DUAL";
	public static final String SYSTIMESTAMP = "SYSTIMESTAMP";
	public static final String SYSTIMESTAMP_FORMAT = "SYSTIMESTAMP_FORMAT";
	public static final String DD_MM_YYYY = "DD_MM_YYYY";
	public static final String FN_UNIX_TIME_TO_DATE = "FN_UNIX_TIME_TO_DATE";
	public static final String TO_DATE_NO_TIMESTAMP = "TO_DATE_NO_TIMESTAMP";
	public static final String TO_DATE_NO_TIMESTAMP_VAL = "TO_DATE_NO_TIMESTAMP_VAL";
	public static final String CAST_AS_DATE = "CAST_AS_DATE";
	public static final String TO_DATE_MM = "TO_DATE_MM";
	public static final String DATEDIFF = "DATEDIFF";
	public static final String FN_UNIX_TIME_TO_DATE_GRID1 = "FN_UNIX_TIME_TO_DATE_GRID1";
	
	/* DB Function conversion Strings - End */
}