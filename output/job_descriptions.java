// ORM class for table 'job_descriptions'
// WARNING: This class is AUTO-GENERATED. Modify at your own risk.
//
// Debug information:
// Generated date: Mon Apr 21 18:27:28 MSK 2025
// For connector: org.apache.sqoop.manager.PostgresqlManager
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.lib.db.DBWritable;
import org.apache.sqoop.lib.JdbcWritableBridge;
import org.apache.sqoop.lib.DelimiterSet;
import org.apache.sqoop.lib.FieldFormatter;
import org.apache.sqoop.lib.RecordParser;
import org.apache.sqoop.lib.BooleanParser;
import org.apache.sqoop.lib.BlobRef;
import org.apache.sqoop.lib.ClobRef;
import org.apache.sqoop.lib.LargeObjectLoader;
import org.apache.sqoop.lib.SqoopRecord;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.HashMap;

public class job_descriptions extends SqoopRecord  implements DBWritable, Writable {
  private final int PROTOCOL_VERSION = 3;
  public int getClassFormatVersion() { return PROTOCOL_VERSION; }
  public static interface FieldSetterCommand {    void setField(Object value);  }  protected ResultSet __cur_result_set;
  private Map<String, FieldSetterCommand> setters = new HashMap<String, FieldSetterCommand>();
  private void init0() {
    setters.put("id", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        job_descriptions.this.id = (Integer)value;
      }
    });
    setters.put("job_id", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        job_descriptions.this.job_id = (Long)value;
      }
    });
    setters.put("experience", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        job_descriptions.this.experience = (String)value;
      }
    });
    setters.put("qualifications", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        job_descriptions.this.qualifications = (String)value;
      }
    });
    setters.put("salary_range", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        job_descriptions.this.salary_range = (String)value;
      }
    });
    setters.put("location", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        job_descriptions.this.location = (String)value;
      }
    });
    setters.put("country", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        job_descriptions.this.country = (String)value;
      }
    });
    setters.put("latitude", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        job_descriptions.this.latitude = (java.math.BigDecimal)value;
      }
    });
    setters.put("longitude", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        job_descriptions.this.longitude = (java.math.BigDecimal)value;
      }
    });
    setters.put("work_type", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        job_descriptions.this.work_type = (String)value;
      }
    });
    setters.put("company_size", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        job_descriptions.this.company_size = (Integer)value;
      }
    });
    setters.put("job_posting_date", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        job_descriptions.this.job_posting_date = (java.sql.Date)value;
      }
    });
    setters.put("preference", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        job_descriptions.this.preference = (String)value;
      }
    });
    setters.put("contact_person", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        job_descriptions.this.contact_person = (String)value;
      }
    });
    setters.put("contact", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        job_descriptions.this.contact = (String)value;
      }
    });
    setters.put("job_title", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        job_descriptions.this.job_title = (String)value;
      }
    });
    setters.put("role", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        job_descriptions.this.role = (String)value;
      }
    });
    setters.put("job_portal", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        job_descriptions.this.job_portal = (String)value;
      }
    });
    setters.put("job_description", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        job_descriptions.this.job_description = (String)value;
      }
    });
    setters.put("benefits", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        job_descriptions.this.benefits = (String)value;
      }
    });
    setters.put("skills", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        job_descriptions.this.skills = (String)value;
      }
    });
    setters.put("responsibilities", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        job_descriptions.this.responsibilities = (String)value;
      }
    });
    setters.put("company_name", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        job_descriptions.this.company_name = (String)value;
      }
    });
    setters.put("company_profile", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        job_descriptions.this.company_profile = (String)value;
      }
    });
  }
  public job_descriptions() {
    init0();
  }
  private Integer id;
  public Integer get_id() {
    return id;
  }
  public void set_id(Integer id) {
    this.id = id;
  }
  public job_descriptions with_id(Integer id) {
    this.id = id;
    return this;
  }
  private Long job_id;
  public Long get_job_id() {
    return job_id;
  }
  public void set_job_id(Long job_id) {
    this.job_id = job_id;
  }
  public job_descriptions with_job_id(Long job_id) {
    this.job_id = job_id;
    return this;
  }
  private String experience;
  public String get_experience() {
    return experience;
  }
  public void set_experience(String experience) {
    this.experience = experience;
  }
  public job_descriptions with_experience(String experience) {
    this.experience = experience;
    return this;
  }
  private String qualifications;
  public String get_qualifications() {
    return qualifications;
  }
  public void set_qualifications(String qualifications) {
    this.qualifications = qualifications;
  }
  public job_descriptions with_qualifications(String qualifications) {
    this.qualifications = qualifications;
    return this;
  }
  private String salary_range;
  public String get_salary_range() {
    return salary_range;
  }
  public void set_salary_range(String salary_range) {
    this.salary_range = salary_range;
  }
  public job_descriptions with_salary_range(String salary_range) {
    this.salary_range = salary_range;
    return this;
  }
  private String location;
  public String get_location() {
    return location;
  }
  public void set_location(String location) {
    this.location = location;
  }
  public job_descriptions with_location(String location) {
    this.location = location;
    return this;
  }
  private String country;
  public String get_country() {
    return country;
  }
  public void set_country(String country) {
    this.country = country;
  }
  public job_descriptions with_country(String country) {
    this.country = country;
    return this;
  }
  private java.math.BigDecimal latitude;
  public java.math.BigDecimal get_latitude() {
    return latitude;
  }
  public void set_latitude(java.math.BigDecimal latitude) {
    this.latitude = latitude;
  }
  public job_descriptions with_latitude(java.math.BigDecimal latitude) {
    this.latitude = latitude;
    return this;
  }
  private java.math.BigDecimal longitude;
  public java.math.BigDecimal get_longitude() {
    return longitude;
  }
  public void set_longitude(java.math.BigDecimal longitude) {
    this.longitude = longitude;
  }
  public job_descriptions with_longitude(java.math.BigDecimal longitude) {
    this.longitude = longitude;
    return this;
  }
  private String work_type;
  public String get_work_type() {
    return work_type;
  }
  public void set_work_type(String work_type) {
    this.work_type = work_type;
  }
  public job_descriptions with_work_type(String work_type) {
    this.work_type = work_type;
    return this;
  }
  private Integer company_size;
  public Integer get_company_size() {
    return company_size;
  }
  public void set_company_size(Integer company_size) {
    this.company_size = company_size;
  }
  public job_descriptions with_company_size(Integer company_size) {
    this.company_size = company_size;
    return this;
  }
  private java.sql.Date job_posting_date;
  public java.sql.Date get_job_posting_date() {
    return job_posting_date;
  }
  public void set_job_posting_date(java.sql.Date job_posting_date) {
    this.job_posting_date = job_posting_date;
  }
  public job_descriptions with_job_posting_date(java.sql.Date job_posting_date) {
    this.job_posting_date = job_posting_date;
    return this;
  }
  private String preference;
  public String get_preference() {
    return preference;
  }
  public void set_preference(String preference) {
    this.preference = preference;
  }
  public job_descriptions with_preference(String preference) {
    this.preference = preference;
    return this;
  }
  private String contact_person;
  public String get_contact_person() {
    return contact_person;
  }
  public void set_contact_person(String contact_person) {
    this.contact_person = contact_person;
  }
  public job_descriptions with_contact_person(String contact_person) {
    this.contact_person = contact_person;
    return this;
  }
  private String contact;
  public String get_contact() {
    return contact;
  }
  public void set_contact(String contact) {
    this.contact = contact;
  }
  public job_descriptions with_contact(String contact) {
    this.contact = contact;
    return this;
  }
  private String job_title;
  public String get_job_title() {
    return job_title;
  }
  public void set_job_title(String job_title) {
    this.job_title = job_title;
  }
  public job_descriptions with_job_title(String job_title) {
    this.job_title = job_title;
    return this;
  }
  private String role;
  public String get_role() {
    return role;
  }
  public void set_role(String role) {
    this.role = role;
  }
  public job_descriptions with_role(String role) {
    this.role = role;
    return this;
  }
  private String job_portal;
  public String get_job_portal() {
    return job_portal;
  }
  public void set_job_portal(String job_portal) {
    this.job_portal = job_portal;
  }
  public job_descriptions with_job_portal(String job_portal) {
    this.job_portal = job_portal;
    return this;
  }
  private String job_description;
  public String get_job_description() {
    return job_description;
  }
  public void set_job_description(String job_description) {
    this.job_description = job_description;
  }
  public job_descriptions with_job_description(String job_description) {
    this.job_description = job_description;
    return this;
  }
  private String benefits;
  public String get_benefits() {
    return benefits;
  }
  public void set_benefits(String benefits) {
    this.benefits = benefits;
  }
  public job_descriptions with_benefits(String benefits) {
    this.benefits = benefits;
    return this;
  }
  private String skills;
  public String get_skills() {
    return skills;
  }
  public void set_skills(String skills) {
    this.skills = skills;
  }
  public job_descriptions with_skills(String skills) {
    this.skills = skills;
    return this;
  }
  private String responsibilities;
  public String get_responsibilities() {
    return responsibilities;
  }
  public void set_responsibilities(String responsibilities) {
    this.responsibilities = responsibilities;
  }
  public job_descriptions with_responsibilities(String responsibilities) {
    this.responsibilities = responsibilities;
    return this;
  }
  private String company_name;
  public String get_company_name() {
    return company_name;
  }
  public void set_company_name(String company_name) {
    this.company_name = company_name;
  }
  public job_descriptions with_company_name(String company_name) {
    this.company_name = company_name;
    return this;
  }
  private String company_profile;
  public String get_company_profile() {
    return company_profile;
  }
  public void set_company_profile(String company_profile) {
    this.company_profile = company_profile;
  }
  public job_descriptions with_company_profile(String company_profile) {
    this.company_profile = company_profile;
    return this;
  }
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof job_descriptions)) {
      return false;
    }
    job_descriptions that = (job_descriptions) o;
    boolean equal = true;
    equal = equal && (this.id == null ? that.id == null : this.id.equals(that.id));
    equal = equal && (this.job_id == null ? that.job_id == null : this.job_id.equals(that.job_id));
    equal = equal && (this.experience == null ? that.experience == null : this.experience.equals(that.experience));
    equal = equal && (this.qualifications == null ? that.qualifications == null : this.qualifications.equals(that.qualifications));
    equal = equal && (this.salary_range == null ? that.salary_range == null : this.salary_range.equals(that.salary_range));
    equal = equal && (this.location == null ? that.location == null : this.location.equals(that.location));
    equal = equal && (this.country == null ? that.country == null : this.country.equals(that.country));
    equal = equal && (this.latitude == null ? that.latitude == null : this.latitude.equals(that.latitude));
    equal = equal && (this.longitude == null ? that.longitude == null : this.longitude.equals(that.longitude));
    equal = equal && (this.work_type == null ? that.work_type == null : this.work_type.equals(that.work_type));
    equal = equal && (this.company_size == null ? that.company_size == null : this.company_size.equals(that.company_size));
    equal = equal && (this.job_posting_date == null ? that.job_posting_date == null : this.job_posting_date.equals(that.job_posting_date));
    equal = equal && (this.preference == null ? that.preference == null : this.preference.equals(that.preference));
    equal = equal && (this.contact_person == null ? that.contact_person == null : this.contact_person.equals(that.contact_person));
    equal = equal && (this.contact == null ? that.contact == null : this.contact.equals(that.contact));
    equal = equal && (this.job_title == null ? that.job_title == null : this.job_title.equals(that.job_title));
    equal = equal && (this.role == null ? that.role == null : this.role.equals(that.role));
    equal = equal && (this.job_portal == null ? that.job_portal == null : this.job_portal.equals(that.job_portal));
    equal = equal && (this.job_description == null ? that.job_description == null : this.job_description.equals(that.job_description));
    equal = equal && (this.benefits == null ? that.benefits == null : this.benefits.equals(that.benefits));
    equal = equal && (this.skills == null ? that.skills == null : this.skills.equals(that.skills));
    equal = equal && (this.responsibilities == null ? that.responsibilities == null : this.responsibilities.equals(that.responsibilities));
    equal = equal && (this.company_name == null ? that.company_name == null : this.company_name.equals(that.company_name));
    equal = equal && (this.company_profile == null ? that.company_profile == null : this.company_profile.equals(that.company_profile));
    return equal;
  }
  public boolean equals0(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof job_descriptions)) {
      return false;
    }
    job_descriptions that = (job_descriptions) o;
    boolean equal = true;
    equal = equal && (this.id == null ? that.id == null : this.id.equals(that.id));
    equal = equal && (this.job_id == null ? that.job_id == null : this.job_id.equals(that.job_id));
    equal = equal && (this.experience == null ? that.experience == null : this.experience.equals(that.experience));
    equal = equal && (this.qualifications == null ? that.qualifications == null : this.qualifications.equals(that.qualifications));
    equal = equal && (this.salary_range == null ? that.salary_range == null : this.salary_range.equals(that.salary_range));
    equal = equal && (this.location == null ? that.location == null : this.location.equals(that.location));
    equal = equal && (this.country == null ? that.country == null : this.country.equals(that.country));
    equal = equal && (this.latitude == null ? that.latitude == null : this.latitude.equals(that.latitude));
    equal = equal && (this.longitude == null ? that.longitude == null : this.longitude.equals(that.longitude));
    equal = equal && (this.work_type == null ? that.work_type == null : this.work_type.equals(that.work_type));
    equal = equal && (this.company_size == null ? that.company_size == null : this.company_size.equals(that.company_size));
    equal = equal && (this.job_posting_date == null ? that.job_posting_date == null : this.job_posting_date.equals(that.job_posting_date));
    equal = equal && (this.preference == null ? that.preference == null : this.preference.equals(that.preference));
    equal = equal && (this.contact_person == null ? that.contact_person == null : this.contact_person.equals(that.contact_person));
    equal = equal && (this.contact == null ? that.contact == null : this.contact.equals(that.contact));
    equal = equal && (this.job_title == null ? that.job_title == null : this.job_title.equals(that.job_title));
    equal = equal && (this.role == null ? that.role == null : this.role.equals(that.role));
    equal = equal && (this.job_portal == null ? that.job_portal == null : this.job_portal.equals(that.job_portal));
    equal = equal && (this.job_description == null ? that.job_description == null : this.job_description.equals(that.job_description));
    equal = equal && (this.benefits == null ? that.benefits == null : this.benefits.equals(that.benefits));
    equal = equal && (this.skills == null ? that.skills == null : this.skills.equals(that.skills));
    equal = equal && (this.responsibilities == null ? that.responsibilities == null : this.responsibilities.equals(that.responsibilities));
    equal = equal && (this.company_name == null ? that.company_name == null : this.company_name.equals(that.company_name));
    equal = equal && (this.company_profile == null ? that.company_profile == null : this.company_profile.equals(that.company_profile));
    return equal;
  }
  public void readFields(ResultSet __dbResults) throws SQLException {
    this.__cur_result_set = __dbResults;
    this.id = JdbcWritableBridge.readInteger(1, __dbResults);
    this.job_id = JdbcWritableBridge.readLong(2, __dbResults);
    this.experience = JdbcWritableBridge.readString(3, __dbResults);
    this.qualifications = JdbcWritableBridge.readString(4, __dbResults);
    this.salary_range = JdbcWritableBridge.readString(5, __dbResults);
    this.location = JdbcWritableBridge.readString(6, __dbResults);
    this.country = JdbcWritableBridge.readString(7, __dbResults);
    this.latitude = JdbcWritableBridge.readBigDecimal(8, __dbResults);
    this.longitude = JdbcWritableBridge.readBigDecimal(9, __dbResults);
    this.work_type = JdbcWritableBridge.readString(10, __dbResults);
    this.company_size = JdbcWritableBridge.readInteger(11, __dbResults);
    this.job_posting_date = JdbcWritableBridge.readDate(12, __dbResults);
    this.preference = JdbcWritableBridge.readString(13, __dbResults);
    this.contact_person = JdbcWritableBridge.readString(14, __dbResults);
    this.contact = JdbcWritableBridge.readString(15, __dbResults);
    this.job_title = JdbcWritableBridge.readString(16, __dbResults);
    this.role = JdbcWritableBridge.readString(17, __dbResults);
    this.job_portal = JdbcWritableBridge.readString(18, __dbResults);
    this.job_description = JdbcWritableBridge.readString(19, __dbResults);
    this.benefits = JdbcWritableBridge.readString(20, __dbResults);
    this.skills = JdbcWritableBridge.readString(21, __dbResults);
    this.responsibilities = JdbcWritableBridge.readString(22, __dbResults);
    this.company_name = JdbcWritableBridge.readString(23, __dbResults);
    this.company_profile = JdbcWritableBridge.readString(24, __dbResults);
  }
  public void readFields0(ResultSet __dbResults) throws SQLException {
    this.id = JdbcWritableBridge.readInteger(1, __dbResults);
    this.job_id = JdbcWritableBridge.readLong(2, __dbResults);
    this.experience = JdbcWritableBridge.readString(3, __dbResults);
    this.qualifications = JdbcWritableBridge.readString(4, __dbResults);
    this.salary_range = JdbcWritableBridge.readString(5, __dbResults);
    this.location = JdbcWritableBridge.readString(6, __dbResults);
    this.country = JdbcWritableBridge.readString(7, __dbResults);
    this.latitude = JdbcWritableBridge.readBigDecimal(8, __dbResults);
    this.longitude = JdbcWritableBridge.readBigDecimal(9, __dbResults);
    this.work_type = JdbcWritableBridge.readString(10, __dbResults);
    this.company_size = JdbcWritableBridge.readInteger(11, __dbResults);
    this.job_posting_date = JdbcWritableBridge.readDate(12, __dbResults);
    this.preference = JdbcWritableBridge.readString(13, __dbResults);
    this.contact_person = JdbcWritableBridge.readString(14, __dbResults);
    this.contact = JdbcWritableBridge.readString(15, __dbResults);
    this.job_title = JdbcWritableBridge.readString(16, __dbResults);
    this.role = JdbcWritableBridge.readString(17, __dbResults);
    this.job_portal = JdbcWritableBridge.readString(18, __dbResults);
    this.job_description = JdbcWritableBridge.readString(19, __dbResults);
    this.benefits = JdbcWritableBridge.readString(20, __dbResults);
    this.skills = JdbcWritableBridge.readString(21, __dbResults);
    this.responsibilities = JdbcWritableBridge.readString(22, __dbResults);
    this.company_name = JdbcWritableBridge.readString(23, __dbResults);
    this.company_profile = JdbcWritableBridge.readString(24, __dbResults);
  }
  public void loadLargeObjects(LargeObjectLoader __loader)
      throws SQLException, IOException, InterruptedException {
  }
  public void loadLargeObjects0(LargeObjectLoader __loader)
      throws SQLException, IOException, InterruptedException {
  }
  public void write(PreparedStatement __dbStmt) throws SQLException {
    write(__dbStmt, 0);
  }

  public int write(PreparedStatement __dbStmt, int __off) throws SQLException {
    JdbcWritableBridge.writeInteger(id, 1 + __off, 4, __dbStmt);
    JdbcWritableBridge.writeLong(job_id, 2 + __off, -5, __dbStmt);
    JdbcWritableBridge.writeString(experience, 3 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(qualifications, 4 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(salary_range, 5 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(location, 6 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(country, 7 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(latitude, 8 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(longitude, 9 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(work_type, 10 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeInteger(company_size, 11 + __off, 4, __dbStmt);
    JdbcWritableBridge.writeDate(job_posting_date, 12 + __off, 91, __dbStmt);
    JdbcWritableBridge.writeString(preference, 13 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(contact_person, 14 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(contact, 15 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(job_title, 16 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(role, 17 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(job_portal, 18 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(job_description, 19 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(benefits, 20 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(skills, 21 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(responsibilities, 22 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(company_name, 23 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(company_profile, 24 + __off, 12, __dbStmt);
    return 24;
  }
  public void write0(PreparedStatement __dbStmt, int __off) throws SQLException {
    JdbcWritableBridge.writeInteger(id, 1 + __off, 4, __dbStmt);
    JdbcWritableBridge.writeLong(job_id, 2 + __off, -5, __dbStmt);
    JdbcWritableBridge.writeString(experience, 3 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(qualifications, 4 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(salary_range, 5 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(location, 6 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(country, 7 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(latitude, 8 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(longitude, 9 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(work_type, 10 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeInteger(company_size, 11 + __off, 4, __dbStmt);
    JdbcWritableBridge.writeDate(job_posting_date, 12 + __off, 91, __dbStmt);
    JdbcWritableBridge.writeString(preference, 13 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(contact_person, 14 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(contact, 15 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(job_title, 16 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(role, 17 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(job_portal, 18 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(job_description, 19 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(benefits, 20 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(skills, 21 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(responsibilities, 22 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(company_name, 23 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(company_profile, 24 + __off, 12, __dbStmt);
  }
  public void readFields(DataInput __dataIn) throws IOException {
this.readFields0(__dataIn);  }
  public void readFields0(DataInput __dataIn) throws IOException {
    if (__dataIn.readBoolean()) { 
        this.id = null;
    } else {
    this.id = Integer.valueOf(__dataIn.readInt());
    }
    if (__dataIn.readBoolean()) { 
        this.job_id = null;
    } else {
    this.job_id = Long.valueOf(__dataIn.readLong());
    }
    if (__dataIn.readBoolean()) { 
        this.experience = null;
    } else {
    this.experience = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.qualifications = null;
    } else {
    this.qualifications = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.salary_range = null;
    } else {
    this.salary_range = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.location = null;
    } else {
    this.location = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.country = null;
    } else {
    this.country = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.latitude = null;
    } else {
    this.latitude = org.apache.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.longitude = null;
    } else {
    this.longitude = org.apache.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.work_type = null;
    } else {
    this.work_type = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.company_size = null;
    } else {
    this.company_size = Integer.valueOf(__dataIn.readInt());
    }
    if (__dataIn.readBoolean()) { 
        this.job_posting_date = null;
    } else {
    this.job_posting_date = new Date(__dataIn.readLong());
    }
    if (__dataIn.readBoolean()) { 
        this.preference = null;
    } else {
    this.preference = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.contact_person = null;
    } else {
    this.contact_person = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.contact = null;
    } else {
    this.contact = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.job_title = null;
    } else {
    this.job_title = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.role = null;
    } else {
    this.role = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.job_portal = null;
    } else {
    this.job_portal = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.job_description = null;
    } else {
    this.job_description = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.benefits = null;
    } else {
    this.benefits = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.skills = null;
    } else {
    this.skills = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.responsibilities = null;
    } else {
    this.responsibilities = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.company_name = null;
    } else {
    this.company_name = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.company_profile = null;
    } else {
    this.company_profile = Text.readString(__dataIn);
    }
  }
  public void write(DataOutput __dataOut) throws IOException {
    if (null == this.id) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeInt(this.id);
    }
    if (null == this.job_id) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeLong(this.job_id);
    }
    if (null == this.experience) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, experience);
    }
    if (null == this.qualifications) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, qualifications);
    }
    if (null == this.salary_range) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, salary_range);
    }
    if (null == this.location) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, location);
    }
    if (null == this.country) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, country);
    }
    if (null == this.latitude) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    org.apache.sqoop.lib.BigDecimalSerializer.write(this.latitude, __dataOut);
    }
    if (null == this.longitude) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    org.apache.sqoop.lib.BigDecimalSerializer.write(this.longitude, __dataOut);
    }
    if (null == this.work_type) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, work_type);
    }
    if (null == this.company_size) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeInt(this.company_size);
    }
    if (null == this.job_posting_date) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeLong(this.job_posting_date.getTime());
    }
    if (null == this.preference) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, preference);
    }
    if (null == this.contact_person) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, contact_person);
    }
    if (null == this.contact) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, contact);
    }
    if (null == this.job_title) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, job_title);
    }
    if (null == this.role) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, role);
    }
    if (null == this.job_portal) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, job_portal);
    }
    if (null == this.job_description) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, job_description);
    }
    if (null == this.benefits) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, benefits);
    }
    if (null == this.skills) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, skills);
    }
    if (null == this.responsibilities) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, responsibilities);
    }
    if (null == this.company_name) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, company_name);
    }
    if (null == this.company_profile) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, company_profile);
    }
  }
  public void write0(DataOutput __dataOut) throws IOException {
    if (null == this.id) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeInt(this.id);
    }
    if (null == this.job_id) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeLong(this.job_id);
    }
    if (null == this.experience) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, experience);
    }
    if (null == this.qualifications) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, qualifications);
    }
    if (null == this.salary_range) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, salary_range);
    }
    if (null == this.location) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, location);
    }
    if (null == this.country) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, country);
    }
    if (null == this.latitude) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    org.apache.sqoop.lib.BigDecimalSerializer.write(this.latitude, __dataOut);
    }
    if (null == this.longitude) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    org.apache.sqoop.lib.BigDecimalSerializer.write(this.longitude, __dataOut);
    }
    if (null == this.work_type) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, work_type);
    }
    if (null == this.company_size) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeInt(this.company_size);
    }
    if (null == this.job_posting_date) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeLong(this.job_posting_date.getTime());
    }
    if (null == this.preference) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, preference);
    }
    if (null == this.contact_person) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, contact_person);
    }
    if (null == this.contact) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, contact);
    }
    if (null == this.job_title) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, job_title);
    }
    if (null == this.role) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, role);
    }
    if (null == this.job_portal) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, job_portal);
    }
    if (null == this.job_description) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, job_description);
    }
    if (null == this.benefits) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, benefits);
    }
    if (null == this.skills) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, skills);
    }
    if (null == this.responsibilities) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, responsibilities);
    }
    if (null == this.company_name) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, company_name);
    }
    if (null == this.company_profile) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, company_profile);
    }
  }
  private static final DelimiterSet __outputDelimiters = new DelimiterSet((char) 44, (char) 10, (char) 0, (char) 0, false);
  public String toString() {
    return toString(__outputDelimiters, true);
  }
  public String toString(DelimiterSet delimiters) {
    return toString(delimiters, true);
  }
  public String toString(boolean useRecordDelim) {
    return toString(__outputDelimiters, useRecordDelim);
  }
  public String toString(DelimiterSet delimiters, boolean useRecordDelim) {
    StringBuilder __sb = new StringBuilder();
    char fieldDelim = delimiters.getFieldsTerminatedBy();
    __sb.append(FieldFormatter.escapeAndEnclose(id==null?"null":"" + id, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(job_id==null?"null":"" + job_id, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(experience==null?"null":experience, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(qualifications==null?"null":qualifications, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(salary_range==null?"null":salary_range, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(location==null?"null":location, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(country==null?"null":country, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(latitude==null?"null":latitude.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(longitude==null?"null":longitude.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(work_type==null?"null":work_type, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(company_size==null?"null":"" + company_size, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(job_posting_date==null?"null":"" + job_posting_date, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(preference==null?"null":preference, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(contact_person==null?"null":contact_person, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(contact==null?"null":contact, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(job_title==null?"null":job_title, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(role==null?"null":role, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(job_portal==null?"null":job_portal, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(job_description==null?"null":job_description, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(benefits==null?"null":benefits, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(skills==null?"null":skills, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(responsibilities==null?"null":responsibilities, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(company_name==null?"null":company_name, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(company_profile==null?"null":company_profile, delimiters));
    if (useRecordDelim) {
      __sb.append(delimiters.getLinesTerminatedBy());
    }
    return __sb.toString();
  }
  public void toString0(DelimiterSet delimiters, StringBuilder __sb, char fieldDelim) {
    __sb.append(FieldFormatter.escapeAndEnclose(id==null?"null":"" + id, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(job_id==null?"null":"" + job_id, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(experience==null?"null":experience, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(qualifications==null?"null":qualifications, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(salary_range==null?"null":salary_range, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(location==null?"null":location, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(country==null?"null":country, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(latitude==null?"null":latitude.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(longitude==null?"null":longitude.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(work_type==null?"null":work_type, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(company_size==null?"null":"" + company_size, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(job_posting_date==null?"null":"" + job_posting_date, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(preference==null?"null":preference, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(contact_person==null?"null":contact_person, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(contact==null?"null":contact, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(job_title==null?"null":job_title, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(role==null?"null":role, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(job_portal==null?"null":job_portal, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(job_description==null?"null":job_description, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(benefits==null?"null":benefits, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(skills==null?"null":skills, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(responsibilities==null?"null":responsibilities, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(company_name==null?"null":company_name, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(company_profile==null?"null":company_profile, delimiters));
  }
  private static final DelimiterSet __inputDelimiters = new DelimiterSet((char) 44, (char) 10, (char) 0, (char) 0, false);
  private RecordParser __parser;
  public void parse(Text __record) throws RecordParser.ParseError {
    if (null == this.__parser) {
      this.__parser = new RecordParser(__inputDelimiters);
    }
    List<String> __fields = this.__parser.parseRecord(__record);
    __loadFromFields(__fields);
  }

  public void parse(CharSequence __record) throws RecordParser.ParseError {
    if (null == this.__parser) {
      this.__parser = new RecordParser(__inputDelimiters);
    }
    List<String> __fields = this.__parser.parseRecord(__record);
    __loadFromFields(__fields);
  }

  public void parse(byte [] __record) throws RecordParser.ParseError {
    if (null == this.__parser) {
      this.__parser = new RecordParser(__inputDelimiters);
    }
    List<String> __fields = this.__parser.parseRecord(__record);
    __loadFromFields(__fields);
  }

  public void parse(char [] __record) throws RecordParser.ParseError {
    if (null == this.__parser) {
      this.__parser = new RecordParser(__inputDelimiters);
    }
    List<String> __fields = this.__parser.parseRecord(__record);
    __loadFromFields(__fields);
  }

  public void parse(ByteBuffer __record) throws RecordParser.ParseError {
    if (null == this.__parser) {
      this.__parser = new RecordParser(__inputDelimiters);
    }
    List<String> __fields = this.__parser.parseRecord(__record);
    __loadFromFields(__fields);
  }

  public void parse(CharBuffer __record) throws RecordParser.ParseError {
    if (null == this.__parser) {
      this.__parser = new RecordParser(__inputDelimiters);
    }
    List<String> __fields = this.__parser.parseRecord(__record);
    __loadFromFields(__fields);
  }

  private void __loadFromFields(List<String> fields) {
    Iterator<String> __it = fields.listIterator();
    String __cur_str = null;
    try {
    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.id = null; } else {
      this.id = Integer.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.job_id = null; } else {
      this.job_id = Long.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null")) { this.experience = null; } else {
      this.experience = __cur_str;
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null")) { this.qualifications = null; } else {
      this.qualifications = __cur_str;
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null")) { this.salary_range = null; } else {
      this.salary_range = __cur_str;
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null")) { this.location = null; } else {
      this.location = __cur_str;
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null")) { this.country = null; } else {
      this.country = __cur_str;
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.latitude = null; } else {
      this.latitude = new java.math.BigDecimal(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.longitude = null; } else {
      this.longitude = new java.math.BigDecimal(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null")) { this.work_type = null; } else {
      this.work_type = __cur_str;
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.company_size = null; } else {
      this.company_size = Integer.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.job_posting_date = null; } else {
      this.job_posting_date = java.sql.Date.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null")) { this.preference = null; } else {
      this.preference = __cur_str;
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null")) { this.contact_person = null; } else {
      this.contact_person = __cur_str;
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null")) { this.contact = null; } else {
      this.contact = __cur_str;
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null")) { this.job_title = null; } else {
      this.job_title = __cur_str;
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null")) { this.role = null; } else {
      this.role = __cur_str;
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null")) { this.job_portal = null; } else {
      this.job_portal = __cur_str;
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null")) { this.job_description = null; } else {
      this.job_description = __cur_str;
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null")) { this.benefits = null; } else {
      this.benefits = __cur_str;
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null")) { this.skills = null; } else {
      this.skills = __cur_str;
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null")) { this.responsibilities = null; } else {
      this.responsibilities = __cur_str;
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null")) { this.company_name = null; } else {
      this.company_name = __cur_str;
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null")) { this.company_profile = null; } else {
      this.company_profile = __cur_str;
    }

    } catch (RuntimeException e) {    throw new RuntimeException("Can't parse input data: '" + __cur_str + "'", e);    }  }

  private void __loadFromFields0(Iterator<String> __it) {
    String __cur_str = null;
    try {
    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.id = null; } else {
      this.id = Integer.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.job_id = null; } else {
      this.job_id = Long.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null")) { this.experience = null; } else {
      this.experience = __cur_str;
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null")) { this.qualifications = null; } else {
      this.qualifications = __cur_str;
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null")) { this.salary_range = null; } else {
      this.salary_range = __cur_str;
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null")) { this.location = null; } else {
      this.location = __cur_str;
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null")) { this.country = null; } else {
      this.country = __cur_str;
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.latitude = null; } else {
      this.latitude = new java.math.BigDecimal(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.longitude = null; } else {
      this.longitude = new java.math.BigDecimal(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null")) { this.work_type = null; } else {
      this.work_type = __cur_str;
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.company_size = null; } else {
      this.company_size = Integer.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.job_posting_date = null; } else {
      this.job_posting_date = java.sql.Date.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null")) { this.preference = null; } else {
      this.preference = __cur_str;
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null")) { this.contact_person = null; } else {
      this.contact_person = __cur_str;
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null")) { this.contact = null; } else {
      this.contact = __cur_str;
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null")) { this.job_title = null; } else {
      this.job_title = __cur_str;
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null")) { this.role = null; } else {
      this.role = __cur_str;
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null")) { this.job_portal = null; } else {
      this.job_portal = __cur_str;
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null")) { this.job_description = null; } else {
      this.job_description = __cur_str;
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null")) { this.benefits = null; } else {
      this.benefits = __cur_str;
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null")) { this.skills = null; } else {
      this.skills = __cur_str;
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null")) { this.responsibilities = null; } else {
      this.responsibilities = __cur_str;
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null")) { this.company_name = null; } else {
      this.company_name = __cur_str;
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null")) { this.company_profile = null; } else {
      this.company_profile = __cur_str;
    }

    } catch (RuntimeException e) {    throw new RuntimeException("Can't parse input data: '" + __cur_str + "'", e);    }  }

  public Object clone() throws CloneNotSupportedException {
    job_descriptions o = (job_descriptions) super.clone();
    o.job_posting_date = (o.job_posting_date != null) ? (java.sql.Date) o.job_posting_date.clone() : null;
    return o;
  }

  public void clone0(job_descriptions o) throws CloneNotSupportedException {
    o.job_posting_date = (o.job_posting_date != null) ? (java.sql.Date) o.job_posting_date.clone() : null;
  }

  public Map<String, Object> getFieldMap() {
    Map<String, Object> __sqoop$field_map = new HashMap<String, Object>();
    __sqoop$field_map.put("id", this.id);
    __sqoop$field_map.put("job_id", this.job_id);
    __sqoop$field_map.put("experience", this.experience);
    __sqoop$field_map.put("qualifications", this.qualifications);
    __sqoop$field_map.put("salary_range", this.salary_range);
    __sqoop$field_map.put("location", this.location);
    __sqoop$field_map.put("country", this.country);
    __sqoop$field_map.put("latitude", this.latitude);
    __sqoop$field_map.put("longitude", this.longitude);
    __sqoop$field_map.put("work_type", this.work_type);
    __sqoop$field_map.put("company_size", this.company_size);
    __sqoop$field_map.put("job_posting_date", this.job_posting_date);
    __sqoop$field_map.put("preference", this.preference);
    __sqoop$field_map.put("contact_person", this.contact_person);
    __sqoop$field_map.put("contact", this.contact);
    __sqoop$field_map.put("job_title", this.job_title);
    __sqoop$field_map.put("role", this.role);
    __sqoop$field_map.put("job_portal", this.job_portal);
    __sqoop$field_map.put("job_description", this.job_description);
    __sqoop$field_map.put("benefits", this.benefits);
    __sqoop$field_map.put("skills", this.skills);
    __sqoop$field_map.put("responsibilities", this.responsibilities);
    __sqoop$field_map.put("company_name", this.company_name);
    __sqoop$field_map.put("company_profile", this.company_profile);
    return __sqoop$field_map;
  }

  public void getFieldMap0(Map<String, Object> __sqoop$field_map) {
    __sqoop$field_map.put("id", this.id);
    __sqoop$field_map.put("job_id", this.job_id);
    __sqoop$field_map.put("experience", this.experience);
    __sqoop$field_map.put("qualifications", this.qualifications);
    __sqoop$field_map.put("salary_range", this.salary_range);
    __sqoop$field_map.put("location", this.location);
    __sqoop$field_map.put("country", this.country);
    __sqoop$field_map.put("latitude", this.latitude);
    __sqoop$field_map.put("longitude", this.longitude);
    __sqoop$field_map.put("work_type", this.work_type);
    __sqoop$field_map.put("company_size", this.company_size);
    __sqoop$field_map.put("job_posting_date", this.job_posting_date);
    __sqoop$field_map.put("preference", this.preference);
    __sqoop$field_map.put("contact_person", this.contact_person);
    __sqoop$field_map.put("contact", this.contact);
    __sqoop$field_map.put("job_title", this.job_title);
    __sqoop$field_map.put("role", this.role);
    __sqoop$field_map.put("job_portal", this.job_portal);
    __sqoop$field_map.put("job_description", this.job_description);
    __sqoop$field_map.put("benefits", this.benefits);
    __sqoop$field_map.put("skills", this.skills);
    __sqoop$field_map.put("responsibilities", this.responsibilities);
    __sqoop$field_map.put("company_name", this.company_name);
    __sqoop$field_map.put("company_profile", this.company_profile);
  }

  public void setField(String __fieldName, Object __fieldVal) {
    if (!setters.containsKey(__fieldName)) {
      throw new RuntimeException("No such field:"+__fieldName);
    }
    setters.get(__fieldName).setField(__fieldVal);
  }

}
