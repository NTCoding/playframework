package play.core.server.netty

import akka.stream.Materializer
import akka.util.ByteString
import org.jboss.netty.buffer.ChannelBuffers
import org.jboss.netty.channel._
import org.jboss.netty.handler.codec.http._
import org.jboss.netty.handler.codec.http.HttpHeaders._
import org.jboss.netty.handler.codec.http.websocketx._
import org.jboss.netty.handler.codec.frame.TooLongFrameException
import org.jboss.netty.handler.ssl._

import org.jboss.netty.channel.group._
import org.reactivestreams.{ Subscription, Subscriber, Publisher }
import play.api._
import play.api.http.websocket._
import play.api.http.{ HttpErrorHandler, DefaultHttpErrorHandler }
import play.api.libs.streams.{ Streams, Accumulator }
import play.api.mvc._
import play.api.libs.iteratee._
import play.api.libs.iteratee.Input._
import play.core.server.NettyServer
import play.core.server.common.{ WebSocketFlowHandler, ForwardedHeaderHandler, ServerRequestUtils, ServerResultUtils }
import play.core.system.RequestIdProvider
import scala.collection.JavaConverters._
import scala.util.control.{ NonFatal, Exception }
import com.typesafe.netty.http.pipelining.{ OrderedDownstreamChannelEvent, OrderedUpstreamMessageEvent }
import scala.concurrent.Future
import java.net.{ InetSocketAddress, URI }
import java.io.IOException

class RequestHandlerSelector(private val server: NettyServer) {

  def selectFor(nettyRequest: HttpRequest) = {
    var nettyVersion = nettyHttpRequest.getProtocolVersion
    val nettyUri = new QueryStringDecoder(nettyHttpRequest.getUri)
    val rHeaders: Headers = getHeaders(nettyHttpRequest)

    Exception
      .allCatch[RequestHeader].either(tryToCreateRequest).fold(
        e => {
          // use unparsed path
          val rh = createRequestHeader(nettyUri.getPath)
          val result = Future
            .successful(()) // Create a dummy future
            .flatMap { _ =>
              // Call errorHandler in another context, don't block here
              errorHandler(server.applicationProvider.get.toOption).onClientError(rh, 400, e.getMessage)
            }(play.api.libs.iteratee.Execution.trampoline)
          (rh, Left(result))
        },
        rh => server.getHandlerFor(rh) match {
          case directResult @ Left(_) => (rh, directResult)
          case Right((taggedRequestHeader, handler, application)) => (taggedRequestHeader, Right((handler, application)))
        })
  }

  private def getHeaders(nettyRequest: HttpRequest): Headers = {
    val pairs = nettyRequest.headers().entries().asScala.map(h => h.getKey -> h.getValue)
    new Headers(pairs)
  }

  private def tryToCreateRequest = {
    val parameters = Map.empty[String, Seq[String]] ++ nettyUri.getParameters.asScala.mapValues(_.asScala)
    // wrapping into URI to handle absoluteURI
    val path = new URI(nettyUri.getPath).getRawPath
    createRequestHeader(path, parameters)
  }

  private def createRequestHeader(parsedPath: String, parameters: Map[String, Seq[String]] = Map.empty[String, Seq[String]]) = {
    //mapping netty request to Play's
    val untaggedRequestHeader = new RequestHeader {
      override val id = RequestIdProvider.requestIDs.incrementAndGet
      override val tags = Map.empty[String, String]
      override def uri = nettyHttpRequest.getUri
      override def path = parsedPath
      override def method = nettyHttpRequest.getMethod.getName
      override def version = nettyVersion.getText
      override def queryString = parameters
      override def headers = rHeaders
      override lazy val remoteAddress = rRemoteAddress
      override lazy val secure = rSecure
    }
    untaggedRequestHeader
  }

  private def errorHandler(app: Option[Application]) = app.fold[HttpErrorHandler](DefaultHttpErrorHandler)(_.errorHandler)

  private def rRemoteAddress = ServerRequestUtils.findRemoteAddress(
    forwardedHeaderHandler,
    rHeaders,
    connectionRemoteAddress = e.getRemoteAddress.asInstanceOf[InetSocketAddress])

  private def rSecure = ServerRequestUtils.findSecureProtocol(
    forwardedHeaderHandler,
    rHeaders,
    connectionSecureProtocol = ctx.getPipeline.get(classOf[SslHandler]) != null)
}