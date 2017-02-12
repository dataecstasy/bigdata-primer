import java.text.SimpleDateFormat;
import java.util.Calendar;

/**
 * 
 */

/**
 * @author vissinha
 *
 */
public class GetDateNow {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		Calendar currentDate = Calendar.getInstance();
		SimpleDateFormat formatter = new SimpleDateFormat("yyyy/MMM/dd HH:mm:ss");
		String dateNow = formatter.format(currentDate.getTime());
		System.out.println("Now the date is:=> "+ dateNow);
	}

}
