import gradio as gr
import tempfile
import os

from util.conf import get_conf
from util.review_paper import review_paper

conf = get_conf()


def review_tab():
    with gr.Tab("论文评审"):
        gr.Markdown("# Paper Review")
        
        with gr.Row():
            # 左侧列：上传PDF和进度
            with gr.Column(scale=1):
                pdf_input = gr.File(
                    label="上传PDF文件",
                    file_types=[".pdf"],
                    type="binary"
                )
                review_button = gr.Button("开始评审", variant="primary")
                progress_output = gr.Markdown("准备就绪")
            
            # 右侧列：显示评审结果
            with gr.Column(scale=2):
                review_output = gr.Markdown()
                copy_button = gr.Button("复制结果")

        def review_wrapper(file):
            if file is None:
                return "请先上传PDF文件", "请先上传PDF文件"
            
            progress_updates = []

            def update_progress(msg):
                if msg not in progress_updates:  # 避免重复消息
                    progress_updates.append(msg)
                    return "\n".join(progress_updates)
            
            try:
                update_progress("1. 开始处理PDF文件...")
                with tempfile.NamedTemporaryFile(suffix='.pdf', delete=False) as temp_pdf:
                    temp_pdf.write(file)
                    temp_path = temp_pdf.name
                
                update_progress("2. PDF文件加载中...")
                
                review_result = None
                for progress in review_paper(temp_path, progress_callback=update_progress):
                    if progress is None:
                        continue
                    if isinstance(progress, tuple):
                        result, stats = progress
                        review_result = result
                        # 直接将结果转换为字符串显示
                        review_text = [
                            "## 评审结果",
                            f"总体评分: {result.get('Overall', 'N/A')}/10",
                            f"决定: {result.get('Decision', 'N/A')}",
                            "",
                            "### 优点",
                            *[f"- {s}" for s in result.get('Strengths', [])],
                            "",
                            "### 缺点",
                            *[f"- {w}" for w in result.get('Weaknesses', [])],
                            "",
                            "### 问题",
                            *[f"- {q}" for q in result.get('Questions', [])],
                            "",
                            "### Token统计",
                            f"- 总计tokens: {stats.get('total_tokens', 0):,}",
                            f"- 预估费用: ¥{stats.get('total_cost', 0):.4f}",
                        ]
                        
                        update_progress("✅ 评审完成!")
                        return "\n".join(review_text), "\n".join(progress_updates)
                    else:
                        progress_text = update_progress(progress)
                        yield "", progress_text
                
                if review_result is None:
                    return "评审未完成", "\n".join(progress_updates)
                    
            except Exception as e:
                error_msg = f"❌ 评审过程出错: {str(e)}"
                update_progress(error_msg)
                return "评审失败", "\n".join(progress_updates)
            finally:
                os.unlink(temp_path)

        review_button.click(
            fn=review_wrapper,
            inputs=[pdf_input],
            outputs=[review_output, progress_output],
            show_progress=True
        )

        # 添加复制功能
        copy_js = """
            (output) => {
                if (!output) return;
                navigator.clipboard.writeText(output);
                const notify = window.notifyOnSuccess || window.notify;
                if (notify) notify({ msg: "已复制到剪贴板！", type: "success" });
            }
        """
        copy_button.click(
            fn=None,
            inputs=[review_output],
            outputs=None,
            js=copy_js
        )
