package model.dao.impl;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import db.config.DbConnect;
import db.exception.DbException;
import model.dao.DepartmentDao;
import model.entities.Department;
import model.entities.Seller;

public class DepartmentDaoJDBC implements DepartmentDao{
	
	private Connection conn;
	
	public DepartmentDaoJDBC(Connection conn){
		this.conn = conn;
	}

	@Override
	public void insert(Department obj) {
		PreparedStatement st = null;
		try {
			st = conn.prepareStatement("Insert into department " + "(name) "
					+ "values (?)", Statement.RETURN_GENERATED_KEYS);

			st.setString(1, obj.getName());

			int rowsAffected = st.executeUpdate();

			if (rowsAffected > 0) {
				ResultSet rs = st.getGeneratedKeys();
				if (rs.next()) {
					int id = rs.getInt(1);
					obj.setId(id);
				}
				DbConnect.closeResultSet(rs);
			}
			else {
				throw new DbException("Unexpected error! No rows affected");
			}
			
		} catch(SQLException e) {
			throw new DbException(e.getMessage());
		} finally {
			DbConnect.closeStatement(st);
		}
	}

	@Override
	public void update(Department obj) {
		
		PreparedStatement st  = null;
		try {
			st = conn.prepareStatement(
					"update department "
					+ "Set Name = ?"
					+ "where id = ?");
			
			st.setString(1, obj.getName());
			st.setInt(2, obj.getId());
			
			st.executeUpdate();
			
		} catch(SQLException e) {
			throw new DbException(e.getMessage());
		} finally {
			DbConnect.closeStatement(st);
		}
		
	}

	@Override
	public void deleteById(Integer id) {
		PreparedStatement st  = null;
		try {
			st = conn.prepareStatement("Delete from department where id = ?");
			st.setInt(1, id);
			
			st.executeUpdate();		
			
		} catch(SQLException e) {
			throw new DbException(e.getMessage());
		} finally {
			DbConnect.closeStatement(st);
		}
	}

	@Override
	public Department findById(Integer id) {

		PreparedStatement st = null;
		ResultSet rs = null;

		try {
			st = conn.prepareStatement(
					"Select * from department where id = ?");

			st.setInt(1, id);
			rs = st.executeQuery();

			if (rs.next()) {
				Department dp = instanceDepartment(rs);
				return dp;
			}

			return null;
		} catch (SQLException e) {
			throw new DbException(e.getMessage());
		}
		finally {
			DbConnect.closeStatement(st);
			DbConnect.closeResultSet(rs);
		}
	}

	@Override
	public List<Department> findAll() {
		PreparedStatement st = null;
		ResultSet rs = null;

		try {
			st = conn.prepareStatement(
					"Select * from department order by id");
			rs = st.executeQuery();
			
			List<Department> list = new ArrayList<>();
			
			while (rs.next()) {
				Department dep = instanceDepartment(rs);
				list.add(dep);
			}

			return list;
			
		} catch (SQLException e) {
			throw new DbException(e.getMessage());
		}
		finally {
			DbConnect.closeStatement(st);
			DbConnect.closeResultSet(rs);
		}
	}
	
	private Department instanceDepartment(ResultSet st) throws SQLException{
		
		Department dp = new Department();
			
		dp.setId(st.getInt("id"));
		dp.setName(st.getString("name"));		
		
		return dp;	
	}

}
