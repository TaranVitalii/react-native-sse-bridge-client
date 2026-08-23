//
//  SSEBridgeClient.m
//  Objective-C registration shim exposing the Swift SSEBridgeClient class to the classic
//  bridge — RCT_EXTERN_MODULE declares the ObjC-visible interface; the actual implementation
//  is the @objc(SSEBridgeClient) Swift class.
//

#import <React/RCTBridgeModule.h>
#import <React/RCTEventEmitter.h>

@interface RCT_EXTERN_MODULE(SSEBridgeClient, RCTEventEmitter)

RCT_EXTERN_METHOD(connect:(NSString *)streamId url:(NSString *)url options:(NSDictionary *)options)
RCT_EXTERN_METHOD(disconnect:(NSString *)streamId)
RCT_EXTERN_METHOD(setEventFilter:(NSString *)streamId types:(NSArray *)types)
RCT_EXTERN_METHOD(setMetricsEnabled:(NSString *)streamId enabled:(BOOL)enabled)

@end
