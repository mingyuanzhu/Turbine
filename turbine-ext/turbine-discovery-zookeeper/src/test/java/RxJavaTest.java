import io.reactivex.Observable;
import org.junit.Test;

/**
 * Created by zhumingyuan on 4/9/17.
 */
public class RxJavaTest {

    @Test
    public void testConcat() {

        Observable.just("a")
            .doFinally(() -> {
                System.out.println("test");
            })
            .subscribe(s -> {
                System.out.println("sub");
            });

    }
}
