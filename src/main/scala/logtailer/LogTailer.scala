package logtailer

import cats.effect.IO
import fs2.Stream
import java.io.{RandomAccessFile}
object LogTailer {

  def tail(path: String): Stream[IO, String] =
    Stream.bracket(IO(new RandomAccessFile(path, "r")))(
      file => {
        file.seek(file.length())
        Stream.eval(IO(file.readLine()))
          .repeat
          .filter(_ != null)
      },
      file => IO(file.close())
    )

  def tailAndFilter(path: String, keyword: String): Stream[IO, String] =
    tail(path).filter(_.contains(keyword))

  def tailMultiple(paths: List[String]): Stream[IO, (String, String)] =
    Stream.emits(paths).flatMap { path =>
      Stream.bracket(IO(new RandomAccessFile(path, "r")))(
        file => {
          file.seek(file.length())
          Stream.eval(IO(file.readLine()))
            .repeat
            .filter(_ != null)
            .map(line => path -> line)
        },
        file => IO(file.close())
      )
    }

}
