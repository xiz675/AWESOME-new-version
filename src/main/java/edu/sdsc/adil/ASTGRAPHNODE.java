/* Generated By:JJTree: Do not edit this line. ASTGRAPHNODE.java Version 7.0 */
/* JavaCCOptions:MULTI=true,NODE_USES_PARSER=false,VISITOR=true,TRACK_TOKENS=false,NODE_PREFIX=AST,NODE_EXTENDS=,NODE_FACTORY=,SUPPORT_CLASS_VISIBILITY_PUBLIC=true */
package edu.sdsc.adil;

public
class ASTGRAPHNODE extends SimpleNode {
  public ASTGRAPHNODE(int id) {
    super(id);
  }

  public ASTGRAPHNODE(Adil p, int id) {
    super(p, id);
  }


  /** Accept the visitor. **/
  public Object jjtAccept(AdilVisitor visitor, Object data) {

    return
    visitor.visit(this, data);
  }
}
/* JavaCC - OriginalChecksum=f85f3ea335e9b665a250f57b4fa49b4c (do not edit this line) */