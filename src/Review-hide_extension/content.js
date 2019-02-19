'use strict';

// chrome.runtime.onInstalled.addListener(function() {
//   chrome.storage.sync.set({color: '#3aa757'}, function() {
//     console.log("The color is green.");
//   });

//   chrome.declarativeContent.onPageChanged.removeRules(undefined, function() {
//       chrome.declarativeContent.onPageChanged.addRules([{
//         conditions: [new chrome.declarativeContent.PageStateMatcher({
//           pageUrl: {hostEquals: 'www.amazon.com'},
//         })
//         ],
//             actions: [new chrome.declarativeContent.ShowPageAction()]
//       }]);
//   });
// });


// on load:
  //    user_ids = gather all user ids and ping server.
  //    for each of user_ids:
  //        if number of reviews > 10:
  //            compute averages, put in relevants_arr
  //            add fields for each of three criteria to relevants_arr


// on update:
//    we're turning on criterium_a:
//        log which criterium to be updated
//        for each of relevants_arr:
//            for each of relevants_arr
//            if match criterium_a:
//                hide user
//            update user[criterium_a] to true
//    else we're turning off criterium_a:
//        for each of relevants_arr:
//            if user[criterium_a] is true and others are false:
//                show user
//            user[criterium_a] = false


$(document).ready(function() {

  var relevants_arr; // This will be list of users who have > 10 reviews, i.e. relevant users.

  var user_ids = {};
  $( "a.a-profile" ).each(function() {
    user_ids[get_url($(this).attr('href'))] =
                                     {
                                       'name':      1,
                                       'num':       1,
                                       'stars':     1,
                                       'too_short': false,
                                       'too_nice':  false,
                                       'too_mean':  false,
                                     };
  });


  // for each input for listener:
       // I'm just going to assume that people with high average reviews aren't
       // the same as those with low ones, and do just two conditionals. I.e. I'm not
       // going to unhide/hide depending on multiple criteria, just update depending on
       // The most recent input.
  //   if clicked off then step through and turn back on

  chrome.runtime.onMessage.addListener(
    function(request, sender, sendResponse) {
      // console.log(sender.tab ?
      //             "from a content script:" + sender.tab.url :
      //             "from the extension");
      // alert(request.selector + ' ' + request.val);
      update(request.selector, request.val);
      sendResponse({farewell: "nope"});
      return true;
    }
  );

  function update(which_selector, val) {
    var reviewers = new Array(); // This will be turned into JSON and sent to the redis interface.
    $( "a.a-profile" ).each(function( idx ) {
      reviewers.push( get_user_id( $(this).attr('href') ) );

      if ($(this).attr('href').includes("AGLHTJEJHQQ763RAURZ2SRP2VKBA")) {
        $(this).parent().parent().css("display", "none");
      }
    });
  }

  function get_user_id(url) {
    var secondidx  = url.search('/ref'); // second index of the substring containing the user id
    return url.substring(26, secondidx); // it always starts at 25
  }


  // Create a url with GET query string for pinging web server
  function get_url() {
    var query_string = '?';
    var idx = 0;
    $( "a.a-profile" ).each(function() {
      query_string += ('id' + idx + '=' + get_user_id( $(this).attr('href') ) + '&');
      idx++;
    });
    return ('https://storystreetconsulting.com/wsgi' + query_string);
  }

  var get_string = get_url(); // Need this to send data to ajax url, because it won't evaluate a fn.

  // send url to server, get response, send it to
  $.ajax({
    dataType: 'json',
    url: get_string,
    success: function(return_data) {
      $.each(return_data, function(index, value) {
        console.log(index + " " + value);
      });

      console.log(JSON.stringify(return_data));
    },
    error: function(xhr, status, errorMsg) {
      $("#results").append("error");
      console.log(errorMsg);
    }
  });


});







  // var xhr = new XMLHttpRequest();
  // xhr.open("POST", 'https://www.storystreetconsulting.com/wsgi', true);
  // xhr.setRequestHeader('Content-Type', 'application/json');
  // xhr.send(JSON.stringify({
  //   value: reviewers
  // }));
  // console.log(JSON.stringify({
  //   value: reviewers
  // }));


// chrome.runtime.onMessage.addListener(
//   function(request, sender, sendResponse) {
//     console.log(sender.tab ?
//                 "from a content script:" + sender.tab.url :
//                 "from the extension");
//     alert(request.greeting);
//     sendResponse({farewell: "goodbye"});
//     // return true;
//   }
// );
