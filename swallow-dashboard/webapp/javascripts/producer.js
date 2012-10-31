(function(w){
var timer;
var swallow={
	"sendMsg":function(async){
		//发送到服务端，如果保存成功,则继续，否则alert错误信息
		var param = new Object();
		param.topic = $("#topic").val();
		param.content = $("#content").val();
		console.log($("#topic").val());
		//发送ajax请求jsonp
		var url;
		if(async != null){
			url = window.contextpath + '/async/producer/sendMsg';			
		}else{
			url = window.contextpath + '/producer/sendMsg';
		}
		
		$.ajax({
			type : 'GET',
			url : url,
			jsonp : 'callback',
			data : param,
			dataType : "jsonp",
			success : swallow.sendMsgDone
		});
		//disable按钮
		$('#sendButton').attr('disabled','disabled');
		//初始化错误消息
		$('#errorMsg > textarea').val('');
		$('#errorMsg').hide();
		//初始化进度条
		$('#progress > label').text('消息发送中...');
		$('#progress > div > div').width('10%');
		$('#progress > div').attr('class','progress progress-info progress-striped active');
		$('#progress').show();
	},
	/**
	 * 发送完成
	 */
	"sendMsgDone":function(data){
		if(data.success==false){
			//显示错误消息
			$('#errorMsg > div[class="modal-body"] > p').text(data.errorMsg);
			//$('#errorMsg').show();
			$('#errorMsg').modal('show');
			//进度条显示成功，然后静止
			$('#progress > div > div').width('100%');
			$('#progress > div').attr('class','progress progress-danger progress-striped active');
			setTimeout(function() {
				$('#progress > label').text('消息发送失败。');
				$('#progress > div').attr('class','progress progress-danger progress-striped');
			}, 500);
			//去掉按钮disable
			$('#sendButton').removeAttr('disabled');
		}else{
			//进度条显示成功，然后静止
			$('#progress > div > div').width('100%');
			$('#progress > div').attr('class','progress progress-success progress-striped active');
			setTimeout(function() {
				$('#progress > label').text('消息发送成功。');
				$('#progress > div').attr('class','progress progress-success progress-striped');
			}, 500);
			//如果启用了连续发送，则再次发送
			if($('#sequenceSend').attr('class')=='btn btn-mini btn-info active'){
				timer = setTimeout(function() {
					swallow.sendMsg();
				}, 1000);
				//如果是连续发送，则不会去掉按钮disable
			}else{
				//如果不是连续发送，则去掉按钮disable
				$('#sendButton').removeAttr('disabled');
			}
			//msgCount++
			$("#msgCount").text(parseInt($("#msgCount").text())+1);
		}
	},
	"cancelSendMsg":function(){
	},
	"sendMsgError":function(){
	}
};
w.swallow = swallow;
}(window || this));
//page loaded
$(document).ready(function(){
	$('#sendButton').removeAttr('disabled');
});

