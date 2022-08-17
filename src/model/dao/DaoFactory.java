package model.dao;

import db.config.DbConnect;
import model.dao.impl.SellerDaoJDBC;

public class DaoFactory {

	public static SellerDao createSellerDao() {
		return new SellerDaoJDBC(DbConnect.getConnection());
	}
	
}
